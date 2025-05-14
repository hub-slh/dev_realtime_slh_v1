package com.slh.function;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

/**
 * @Package com.slh.function.DimBaseCategory
 * @Author song.lihao
 * @Date 2025/5/14 14:26
 * @description:
 */
@AllArgsConstructor
@NoArgsConstructor
@Data
public class DimBaseCategory implements Serializable {

    private String id;
    private String b3name;
    private String b2name;
    private String b1name;
}
