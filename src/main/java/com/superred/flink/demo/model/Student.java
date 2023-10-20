package com.superred.flink.demo.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Desc:
 * Created by QiaoKang on 2023-10-19
 */

@Data
@AllArgsConstructor
@NoArgsConstructor
public class Student {
    public int id;
    public String name;
    public String password;
    public int age;
}
