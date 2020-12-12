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

local construct_job = function(partial, args, enqueued_at, succeeded, dead)
   local status = '{"succeeded":'
   if #succeeded > 0 then
      status = status .. cjson.encode(succeeded) .. ','
   else
      status = status .. '[],'
   end

   status = status .. '"dead":'
   if #dead > 0 then
      status = status .. cjson.encode(dead) .. '}'
   else
      status = status .. '[]}'
   end

   partial = string.sub(partial, 1, -2)
   args = string.sub(args, 1, -2) .. ',' .. status .. ']'
   return partial .. ',"args":' .. args .. ',"enqueued_at":' .. enqueued_at .. '}'
end

local queues_key = KEYS[1]
local callback_job_queue_key = KEYS[2]
local batch_state_key = KEYS[3]
local batch_on_complete_key = KEYS[4]
local batch_jobs_key = KEYS[5]
local batch_successful_jobs_key = KEYS[6]
local batch_dead_jobs_key = KEYS[7]
local jid_to_batch_id_key = KEYS[8]
local callback_job_enqueued_at, jid, status, ttl = ARGV[1], ARGV[2], ARGV[3], ARGV[4]

redis.call('DEL', jid_to_batch_id_key)
if redis.call('SISMEMBER', batch_jobs_key, jid) == 0 then
   return 0
end

if status == 'success' then
   redis.call('SADD', batch_successful_jobs_key, jid)
   redis.call('EXPIRE', batch_successful_jobs_key, ttl)
elseif status == 'dead' then
   redis.call('SADD', batch_dead_jobs_key, jid)
   redis.call('EXPIRE', batch_dead_jobs_key, ttl)
end
local state = redis.call('GET', batch_state_key)
if state == 'initialized' then
   return 0
end


local total_jobs_count = redis.call('SCARD', batch_jobs_key)
local successful_jobs_count = redis.call('SCARD', batch_successful_jobs_key)
local dead_jobs_count = redis.call('SCARD', batch_dead_jobs_key)

if total_jobs_count == successful_jobs_count + dead_jobs_count then
   local on_complete = hgetall(batch_on_complete_key)
   local succeeded = redis.call('SMEMBERS', batch_successful_jobs_key)
   local dead = redis.call('SMEMBERS', batch_dead_jobs_key)
   local callback_job = construct_job(on_complete['job'], on_complete['args'], callback_job_enqueued_at, succeeded, dead)
   redis.call('SADD', queues_key, on_complete['queue'])
   redis.call('LPUSH', callback_job_queue_key, callback_job)
   redis.call('DEL', batch_state_key, batch_on_complete_key, batch_jobs_key, batch_successful_jobs_key, batch_dead_jobs_key)
end

return 0
