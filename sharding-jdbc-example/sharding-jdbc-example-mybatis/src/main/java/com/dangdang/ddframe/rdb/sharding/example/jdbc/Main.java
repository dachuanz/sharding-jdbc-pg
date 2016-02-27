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

package com.dangdang.ddframe.rdb.sharding.example.jdbc;

import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import com.dangdang.ddframe.rdb.sharding.example.jdbc.entity.Order;
import com.dangdang.ddframe.rdb.sharding.example.jdbc.repository.OrderRepository;

public final class Main {
    
    private static ApplicationContext applicationContext;
    
    public static void main(final String[] args) {
        startContainer();
        select();
        System.out.println("--------------");
        selectAll();
    }
    
    private static void startContainer() {
        applicationContext = new ClassPathXmlApplicationContext("META-INF/mybatisContext.xml");
    }
    
    private static void select() {
        Order criteria = new Order();
        criteria.setUserId(10);
        criteria.setOrderId(1000);
        Order model = applicationContext.getBean(OrderRepository.class).selectById(criteria);
        System.out.println(model);
    }
    
    private static void selectAll() {
        System.out.println(applicationContext.getBean(OrderRepository.class).selectAll());
    }
}
