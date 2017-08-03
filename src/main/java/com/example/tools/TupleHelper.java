package com.example.tools;

import org.apache.storm.Constants;
import org.apache.storm.tuple.Tuple;

/**
 * Created by nikzz on 03/08/17.
 */
public final class TupleHelper {

    private TupleHelper() {
    }

    public static boolean isTickTuple(Tuple tuple) {
        return tuple.getSourceComponent().equals(Constants.SYSTEM_COMPONENT_ID) && tuple.getSourceStreamId().equals(
                Constants.SYSTEM_TICK_STREAM_ID);
    }

}

