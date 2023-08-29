/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.polycat.catalog.common.model.record;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.math.BigDecimal;

import io.polycat.catalog.common.exception.CarbonSqlException;
import io.polycat.catalog.common.types.DataType;
import io.polycat.catalog.common.types.DataTypes;

/**
 * map阶段用于计算平均值的中间字段
 * @singe 2020/12/18
 */
public class MapAvgWritable extends Field {
    // 平均值中的和
    private BigDecimal sumValue;
    
    // 平均值中用于除的那个总数
    private int count;

    public MapAvgWritable() {
    }

    public MapAvgWritable(BigDecimal sumValue, int count) {
        this.sumValue = sumValue;
        this.count = count;
    }

    @Override
    public DataType getType() {
        return DataTypes.MAP_AVG_DATA;
    }

    @Override
    public String getString() {
        return sumValue.toString();
    }
    
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(sumValue.toString());
        out.writeInt(count);
    }
    
    @Override
    public void readFields(DataInput in) throws IOException {
        sumValue = new BigDecimal(in.readUTF());
        count = in.readInt();
    }

    public BigDecimal getAvg() {
        return DecimalWritable.decimalDivide(this.getSumValue(), new BigDecimal(this.count)).getDecimal();
    }
    
    @Override
    public int compareTo(Field o) {
        if (!(o instanceof MapAvgWritable)) {
            throw new CarbonSqlException("MapAvgWritable unsupport compare other type:" + o.getType());
        }
        MapAvgWritable other = (MapAvgWritable)o; 
        BigDecimal thisAvg = this.getAvg();
        BigDecimal otherAvg = other.getAvg();
        return thisAvg.compareTo(otherAvg);
    }
    
    public MapAvgWritable add(MapAvgWritable other) {
        return new MapAvgWritable(this.sumValue.add(other.sumValue), this.count + other.count); 
    }

    public BigDecimal getSumValue() {
        return sumValue;
    }

    public int getCount() {
        return count;
    }

    @Override
    public String toString() {
        return "mapAvgData:{" + "sumValue=" + sumValue + ", count=" + count + '}';
    }
}
