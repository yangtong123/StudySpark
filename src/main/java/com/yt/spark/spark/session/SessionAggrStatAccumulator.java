package com.yt.spark.spark.session;

import com.yt.spark.conf.Constants;
import com.yt.spark.util.*;
import org.apache.spark.util.AccumulatorV2;

/**
 * session聚合统计Accumulator
 *
 * Created by yangtong on 17/5/9.
 */
public class SessionAggrStatAccumulator extends AccumulatorV2<String, String> {

    private String result = Constants.SESSION_COUNT + "=0|"
            + Constants.TIME_PERIOD_1s_3s + "=0|"
            + Constants.TIME_PERIOD_4s_6s + "=0|"
            + Constants.TIME_PERIOD_7s_9s + "=0|"
            + Constants.TIME_PERIOD_10s_30s + "=0|"
            + Constants.TIME_PERIOD_30s_60s + "=0|"
            + Constants.TIME_PERIOD_1m_3m + "=0|"
            + Constants.TIME_PERIOD_3m_10m + "=0|"
            + Constants.TIME_PERIOD_10m_30m + "=0|"
            + Constants.TIME_PERIOD_30m + "=0|"
            + Constants.STEP_PERIOD_1_3 + "=0|"
            + Constants.STEP_PERIOD_4_6 + "=0|"
            + Constants.STEP_PERIOD_7_9 + "=0|"
            + Constants.STEP_PERIOD_10_30 + "=0|"
            + Constants.STEP_PERIOD_30_60 + "=0|"
            + Constants.STEP_PERIOD_60 + "=0";

    /**
     * 判断是否为空
     * @return
     */
    @Override
    public boolean isZero() {
        String[] results = result.split("\\|");
        for (String s : results) {
            if(Long.valueOf(s.split("=")[1]) != 0) {
                return false;
            }
        }
        return true;
    }

    /**
     * 复制一份
     * @return
     */
    @Override
    public AccumulatorV2<String, String> copy() {
        SessionAggrStatAccumulator copyAccumulator = new SessionAggrStatAccumulator();
        copyAccumulator.result = this.result;
        return copyAccumulator;
    }

    /**
     * 重设
     */
    @Override
    public void reset() {
        result = Constants.SESSION_COUNT + "=0|"
                + Constants.TIME_PERIOD_1s_3s + "=0|"
                + Constants.TIME_PERIOD_4s_6s + "=0|"
                + Constants.TIME_PERIOD_7s_9s + "=0|"
                + Constants.TIME_PERIOD_10s_30s + "=0|"
                + Constants.TIME_PERIOD_30s_60s + "=0|"
                + Constants.TIME_PERIOD_1m_3m + "=0|"
                + Constants.TIME_PERIOD_3m_10m + "=0|"
                + Constants.TIME_PERIOD_10m_30m + "=0|"
                + Constants.TIME_PERIOD_30m + "=0|"
                + Constants.STEP_PERIOD_1_3 + "=0|"
                + Constants.STEP_PERIOD_4_6 + "=0|"
                + Constants.STEP_PERIOD_7_9 + "=0|"
                + Constants.STEP_PERIOD_10_30 + "=0|"
                + Constants.STEP_PERIOD_30_60 + "=0|"
                + Constants.STEP_PERIOD_60 + "=0";
    }

    /**
     * 加操作
     * @param v
     */
    @Override
    public void add(String v) {
        String v1 = result;
        String v2 = v;

        //在result里找到相对应的字段加1
        if (StringUtils.isNotEmpty(v1) && StringUtils.isNotEmpty(v2)) {
            String oldValue = StringUtils.getFieldFromConcatString(v1, "\\|", v2);
            if (oldValue != null) {
                int newValue = Integer.valueOf(oldValue) + 1;
                result = StringUtils.setFieldInConcatString(v1, "\\|", v2, String.valueOf(newValue));
            }
        }

    }

    /**
     * 合并两个AccumulatorV2
     * @param other
     */
    @Override
    public void merge(AccumulatorV2<String, String> other) {
        if (other == null) {
            return;
        }

        String[] myFields = result.split("\\|");
        String[] otherFields = other.value().split("\\|");

        //对每个字段对应的值相加，并把新值赋给result
        for (int i = 0; i < myFields.length; i++) {
            String myValue = myFields[i].split("=")[1];
            String otherValue = otherFields[i].split("=")[1];
            if (StringUtils.isNotEmpty(myValue) && StringUtils.isNotEmpty(otherValue)) {
                int newValue = Integer.valueOf(myValue) + Integer.valueOf(otherValue);
                result = StringUtils.setFieldInConcatString(result, "\\|", myFields[i].split("=")[0], String.valueOf(newValue));
            }
        }
    }

    /**
     * 得到当前值
     * @return
     */
    @Override
    public String value() {
        return result;
    }
}
