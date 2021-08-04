package util;

import java.math.BigDecimal;

/**
 * @ClassName: Jexl3Utils
 * @Description: 计算工具
 * @Author: hongkuncc
 * @Date 2021/8/3 22:45
 * @Version 1.0
 */
public class Jexl3Utils {
    public BigDecimal ceil(Object value) {
        return new BigDecimal(value.toString()).divide(new BigDecimal(1), 0, BigDecimal.ROUND_CEILING);
    }
    public BigDecimal floor(Object value){
        return new BigDecimal(value.toString()).divide(new BigDecimal(1),0,BigDecimal.ROUND_FLOOR);
    }

}
