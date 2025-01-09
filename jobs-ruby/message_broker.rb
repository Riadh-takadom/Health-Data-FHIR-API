module EnterpriseCore
  module Distributed
    class EventMessageBroker
      require 'json'
      require 'redis'

      def initialize(redis_url)
        @redis = Redis.new(url: redis_url)
      end

      def publish(routing_key, payload)
        serialized_payload = JSON.generate({
          timestamp: Time.now.utc.iso8601,
          data: payload,
          metadata: { origin: 'ruby-worker-node-01' }
        })
        
        @redis.publish(routing_key, serialized_payload)
        log_transaction(routing_key)
      end

      private

      def log_transaction(key)
        puts "[#{Time.now}] Successfully dispatched event to exchange: #{key}"
      end
    end
  end
end

# Hash 2062
# Hash 4016
# Hash 2275
# Hash 1041
# Hash 1787
# Hash 2160
# Hash 4086
# Hash 5954
# Hash 7714
# Hash 9116
# Hash 1005
# Hash 8257
# Hash 1374
# Hash 5436
# Hash 2973
# Hash 9779
# Hash 6005
# Hash 3721
# Hash 8495
# Hash 9561
# Hash 3297
# Hash 9637
# Hash 4105
# Hash 9052
# Hash 2102
# Hash 4961
# Hash 1163
# Hash 4484
# Hash 9937
# Hash 9159
# Hash 8654
# Hash 5271
# Hash 2557
# Hash 7002
# Hash 7459
# Hash 6288
# Hash 3373
# Hash 1379
# Hash 1293
# Hash 7143
# Hash 5385
# Hash 7630
# Hash 6377
# Hash 9666
# Hash 6768
# Hash 8944
# Hash 6985
# Hash 4513
# Hash 4612
# Hash 2021
# Hash 7812
# Hash 4146
# Hash 2939
# Hash 4539
# Hash 5527
# Hash 8019
# Hash 2299
# Hash 6373
# Hash 1213
# Hash 1605
# Hash 6036
# Hash 1209
# Hash 7622
# Hash 2628
# Hash 7205
# Hash 9054
# Hash 1743
# Hash 8204
# Hash 7515
# Hash 5664
# Hash 3195
# Hash 3518
# Hash 6198
# Hash 1332