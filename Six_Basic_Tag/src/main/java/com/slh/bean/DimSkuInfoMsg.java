package com.slh.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

/**
 * @Package com.slh.bean.DimSkuInfoMsg
 * @Author song.lihao
 * @Date 2025/5/15 15:50
 * @description:
 */
@AllArgsConstructor
@NoArgsConstructor
@Data
public class DimSkuInfoMsg implements Serializable {
    private String skuid;
    private String spuid;
    private String c3id;
    private String tname;
}

