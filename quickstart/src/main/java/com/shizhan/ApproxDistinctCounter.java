package com.shizhan;


import com.shizhan.redis.RedisHLL;

import java.util.Scanner;

public class ApproxDistinctCounter {
    public static void main (String args[]){
        while(true){
            Scanner in = new Scanner(System.in);

            System.out.println("Enter an user id:");
            String user_id = in.next();
            System.out.println("How many buckets do you want to get approx count for?");
            String diff = in.next();
            System.out.println(RedisHLL.getCountForLastNBucketsForUser(user_id,diff));
        }
    }
}
