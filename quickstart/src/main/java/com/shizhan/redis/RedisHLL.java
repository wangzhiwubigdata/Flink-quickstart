package com.shizhan.redis;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.util.ArrayList;
import java.util.List;

public class RedisHLL {

    private static final JedisPool pool = new JedisPool(new JedisPoolConfig(), "localhost");

    public static void addValuesForABucket(String userId, String bucketId, String...values){
        Jedis jedis = null;
        try {
            jedis = pool.getResource();
            jedis.set("user_" + userId, bucketId);
            jedis.pfadd(getUserBucketId(userId, bucketId), values);
        }finally{
            if(jedis != null) {
                jedis.close();
            }
        }
    }

    public static Long getCountForLastNBucketsForUser(String userId, String N){
        Jedis jedis = null;
        try {
            jedis = pool.getResource();
            final String val = jedis.get("user_" + userId);
            if (val == null || val.equals("")) {
                return 0l;
            }
            Long startTS = Long.valueOf(val);

            final String merged_key = getUserMergedBucketId(userId, val, N);
            if (!jedis.exists(merged_key)) {
                List<String> existingKeys = new ArrayList();
                Long ts = startTS;
                for (int i = 1; i <= Integer.valueOf(N); i++) {

                    final String bucketKey = getUserBucketId(userId, String.valueOf(ts));
                    if (jedis.exists(bucketKey)) {
                        existingKeys.add(bucketKey);
                    }
                    ts -= 20 * 1000;
                }
                jedis.pfmerge(merged_key, existingKeys.toArray(new String[existingKeys.size()]));
            }
            return jedis.pfcount(merged_key);
        }finally{
            if(jedis != null) {
                jedis.close();
            }
        }
    }

    private static String getUserBucketId(String userId, String bucket_id){
        return "user_" + userId + "_" + bucket_id;
    }

    private static String getUserMergedBucketId(String userId,
                                                String startBucketId, String diff){
        return "user_" + userId + "_" + startBucketId + "_" + diff;
    }
}
