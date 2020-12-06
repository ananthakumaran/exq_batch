local hgetall = function (key)
   local bulk = redis.call('HGETALL', key)
   local result = {}
   local nextkey
   for i, v in ipairs(bulk) do
      if i % 2 == 1 then
         nextkey = v
      else
         result[nextkey] = v
      end
   end
   return result
end

local queues_key = KEYS[1]
local callback_job_queue_key = KEYS[2]
local batch_state_key = KEYS[3]
local batch_on_complete_key = KEYS[4]
local batch_jobs_key = KEYS[5]
local batch_successful_jobs_key = KEYS[6]
local batch_dead_jobs_key = KEYS[7]
local callback_job_enqueued_at, ttl = ARGV[1], ARGV[2]

local total_jobs_count = redis.call('SCARD', batch_jobs_key)
local successful_jobs_count = redis.call('SCARD', batch_successful_jobs_key)
local dead_jobs_count = redis.call('SCARD', batch_dead_jobs_key)

if total_jobs_count == successful_jobs_count + dead_jobs_count then
   local callback_job = hgetall(batch_on_complete_key)
   callback_job['retries'] = tonumber(callback_job['retries'])
   callback_job['enqueued_at'] = callback_job_enqueued_at
   callback_job['args'] = cjson.decode(callback_job['args'])

   local completion_status = {}
   if successful_jobs_count > 0 then
      completion_status['succeeded'] = redis.call('SMEMBERS', batch_successful_jobs_key)
   end

   if dead_jobs_count > 0 then
      completion_status['dead'] = redis.call('SMEMBERS', batch_dead_jobs_key)
   end

   table.insert(callback_job['args'], completion_status)
   redis.call('SADD', queues_key, callback_job['queue'])
   redis.call('LPUSH', callback_job_queue_key, cjson.encode(callback_job))

   redis.call('DEL', batch_state_key, batch_on_complete_key, batch_jobs_key, batch_successful_jobs_key, batch_dead_jobs_key)
else
   redis.call('SETEX', batch_state_key, ttl, 'created')
end

return 0
