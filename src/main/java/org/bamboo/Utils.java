package org.bamboo;

public class Utils {

    public static boolean isZionColumnFamily(ColumnFamily cf) {
        return Constants.ZION_CF.equals(cf.getName());
    }
}
