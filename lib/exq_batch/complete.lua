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

local queues_key, callback_job_queue_key, batch_state_key, batch_on_complete_key, batch_jobs_key, batch_successful_jobs_key, batch_dead_jobs_key = KEYS[1], KEYS[2], KEYS[3], KEYS[4], KEYS[5], KEYS[6], KEYS[7]
local callback_job_enqueued_at, jid, status = ARGV[1], ARGV[2], ARGV[3]

if status == 'success' then
   redis.call('SADD', batch_successful_jobs_key, jid)
elseif status == 'dead' then
   redis.call('SADD', batch_dead_jobs_key, jid)
end

local state = redis.call('GET', batch_state_key)
if state == 'initialized' then
   return 0
end


local total_jobs_count = redis.call('SCARD', batch_jobs_key)
local successful_jobs_count = redis.call('SCARD', batch_successful_jobs_key)
local dead_jobs_count = redis.call('SCARD', batch_dead_jobs_key)

if total_jobs_count == successful_jobs_count + dead_jobs_count then
   local callback_job = hgetall(batch_on_complete_key)
   callback_job['retries'] = tonumber(callback_job['retries'])
   callback_job['enqueued_at'] = callback_job_enqueued_at
   callback_job['args'] = cjson.decode(callback_job['args'])
   redis.call('SADD', queues_key, callback_job['queue'])
   redis.call('LPUSH', callback_job_queue_key, cjson.encode(callback_job))

   redis.call('DEL', batch_state_key, batch_on_complete_key, batch_jobs_key, batch_successful_jobs_key, batch_dead_jobs_key)
end

return 0
