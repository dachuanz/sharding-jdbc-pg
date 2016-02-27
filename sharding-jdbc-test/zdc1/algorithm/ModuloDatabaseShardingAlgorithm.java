package zdctest.zdc1.algorithm;

/**
 * Copyright 1999-2015 dangdang.com.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * </p>
 */



import java.util.Collection;

import com.dangdang.ddframe.rdb.sharding.api.ShardingValue;
import com.dangdang.ddframe.rdb.sharding.api.strategy.database.SingleKeyDatabaseShardingAlgorithm;

import net.oschina.crypto.PartitionUtil;
/**
 * 
 * 数据库切分
 */
public final class ModuloDatabaseShardingAlgorithm implements SingleKeyDatabaseShardingAlgorithm<Long> {
    
    @Override
    public String doEqualSharding(final Collection<String> dataSourceNames, final ShardingValue<Long> shardingValue) {
        for (String each : dataSourceNames) {
        	//shardingValue.getValue()
        	Long string = PartitionUtil.partitioning(shardingValue.getValue().toString(), 1);
            if (each.endsWith(string.toString())) {
                return each;
            }
        }
        throw new IllegalArgumentException();
    }

	@Override
	public Collection<String> doInSharding(Collection<String> availableTargetNames, ShardingValue<Long> shardingValue) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Collection<String> doBetweenSharding(Collection<String> availableTargetNames,
			ShardingValue<Long> shardingValue) {
		// TODO Auto-generated method stub
		return null;
	}
    
    
   
}
