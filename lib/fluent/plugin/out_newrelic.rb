#
# Copyright 2018 - New Relic
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

require 'fluent/plugin/output'
require 'net/http'
require 'uri'
require 'zlib'
require 'newrelic-fluentd-output/version'
require 'msgpack'
require 'yajl'

module Fluent
  module Plugin
    class NewrelicOutput < Fluent::Plugin::Output
      class ConnectionFailure < StandardError
      end
      Fluent::Plugin.register_output('newrelic', self)
      helpers :thread

      config_param :api_key, :string, :default => nil
      config_param :base_uri, :string, :default => "https://log-api.newrelic.com/log/v1"
      config_param :license_key, :string, :default => nil
      config_param :suppress_metrics_warnings, :bool, :default => false

      DEFAULT_BUFFER_TYPE = 'memory'.freeze

      config_section :buffer do
        config_set_default :@type, DEFAULT_BUFFER_TYPE
        config_set_default :chunk_keys, ['timestamp']
      end

      define_method('log') {$log} unless method_defined?(:log)

      # This tells Fluentd that it can run this output plugin in multiple workers.
      # Our plugin has no interactions with other processes
      def multi_workers_ready?
        true
      end

      def configure(conf)
        super

        if @api_key.nil? && @license_key.nil?
          raise Fluent::ConfigError.new("'api_key' or `license_key` parameter is required") 
        end

        # create initial sockets hash and socket based on config param
        @end_point = URI.parse(@base_uri)
        auth = {
          @api_key.nil? ? 'X-License_key' : 'X-Insert-Key' => 
          @api_key.nil? ? @license_key : @api_key 
        }
        @header = {
            'X-Event-Source' => 'logs',
            'Content-Encoding' => 'gzip'
        }.merge(auth)
        .freeze
      end

      def package_record(tag, timestamp, record)
        record['tag'] = tag

        packaged = {
          'timestamp' => timestamp,
          # non-intrinsic attributes get put into 'attributes'
          'attributes' => record
        }

        # intrinsic attributes go at the top level
        if record.has_key?('message')
          packaged['message'] = record['message']
          packaged['attributes'].delete('message')
        end

        # Kubernetes logging puts the message field in the 'log' attribute, we'll use that
        # as the 'message' field if it exists. We do the same in the Fluent Bit output plugin.
        # See https://docs.docker.com/config/containers/logging/fluentd/
        if record.has_key?('log')
          packaged['message'] = record['log']
          packaged['attributes'].delete('log')
        end

        packaged
      end

      def calculate_metrics(entries)
        entries.group_by { |e|
          {
            type: e['attributes']['bindplane_source_type'],
            bundle_config_id: e['attributes']['bindplane_source_id'],
            tag: e['attributes']['tag']
          }
        }.map { |k, v|
          {
            type: k[:type],
            bundleConfigId: k[:bundle_config_id],
            tag: k[:tag],
            log_count: v.length,
            log_bytes: Yajl.dump(v).bytesize
          }
        }
      end

      def write(chunk)
        payload = {
          'common' => {
            'attributes' => {
              'plugin' => {
                'type' => 'fluentd',
                'version' => NewrelicFluentdOutput::VERSION
              }
            }
          },
          'logs' => []
        }
        
        unpacker = MessagePack::Unpacker.new
        unpacker.feed(chunk.read)
        unpacker.each do |entry|
          tag = entry['tag']
          time = entry['time']
          record = entry['record']

          next unless entry['record'].is_a? Hash
          next if entry['record'].empty?

          payload['logs'].push(package_record(tag, time, record))
        end
        io = StringIO.new
        gzip = Zlib::GzipWriter.new(io)
        gzip << Yajl.dump([payload])
        gzip.close
        send_payload(io.string)
        Thread.new { send_log_metrics(payload['logs']) } 
      end

      def handle_response(response)
        return if response.code.to_i >= 200 && response.code.to_i <= 300

        log.error("Response was #{response.code} #{response.body}")
      end

      def send_payload(payload)
        http = Net::HTTP.new(@end_point.host, 443)
        http.use_ssl = true
        http.verify_mode = OpenSSL::SSL::VERIFY_PEER
        request = Net::HTTP::Post.new(@end_point.request_uri, @header)
        request.body = payload
        handle_response(http.request(request))
      end

      def format(tag, time, record)
        MessagePack.pack(tag: tag, time: time, record: record)
      end

      def send_log_metrics(entries)
        begin
          tries ||= 0
          client = TCPSocket.open('127.0.0.1', 25_498)
        rescue StandardError => e
          tries += 1
          if tries < 3
            log.warn "Failed to open connection to Bindplane Log Agent counter API. Retrying #{tries}." unless @suppress_metrics_warnings
            sleep 15
            retry
          end
          raise
        end

        metrics = calculate_metrics(entries)
        client.write(metrics.to_json)
      rescue StandardError => e
        log.error "Failed to send log counts to Bindplane Log Agent. Skipping sending this metrics request" unless @suppress_metrics_warnings
      ensure
        client.close unless client.nil?
      end
    end
  end
end
