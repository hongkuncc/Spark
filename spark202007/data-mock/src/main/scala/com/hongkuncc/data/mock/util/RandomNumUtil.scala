package com.hongkuncc.data.mock.util


import java.util.Random

import scala.collection.mutable

/**
 * Author lzc
 * Date 2019-07-17 14:07
 *
 * 随机生成整数的工具类
 */
object RandomNumUtil {
    val random = new Random()
    
    /**
     * 返回一个随机的整数 [from, to]
     *
     * @param from
     * @param to
     * @return
     */
    def randomInt(from: Int, to: Int): Int = {
        if (from > to) throw new IllegalArgumentException(s"from = $from 应该小于 to = $to")
        // [0, to - from)  + from [form, to -from + from ]
        random.nextInt(to - from + 1) + from
    }
    
    /**
     * 随机的Long  [from, to]
     *
     * @param from
     * @param to
     * @return
     */
    def randomLong(from: Long, to: Long): Long = {
        if (from > to) throw new IllegalArgumentException(s"from = $from 应该小于 to = $to")
        random.nextLong().abs % (to - from + 1) + from
    }
    
    /**
     * 生成一系列的随机值
     *
     * @param from
     * @param to
     * @param count
     * @param canRepeat 是否允许随机数重复
     */
    def randomMultiInt(from: Int, to: Int, count: Int, canRepeat: Boolean = true): List[Int] = {
        if (canRepeat) {
            (1 to count).map(_ => randomInt(from, to)).toList
        } else {
            val set: mutable.Set[Int] = mutable.Set[Int]()
            while (set.size < count) {
                set += randomInt(from, to)
            }
            set.toList
        }
    }
    
    
    def main(args: Array[String]): Unit = {
        println(randomMultiInt(1, 15, 10))
        println(randomMultiInt(1, 8, 6, false))
    }
}

