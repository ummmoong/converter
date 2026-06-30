package com.tagConverter.mapper;

import org.apache.ibatis.annotations.*;

import java.util.List;


@Mapper
public interface TagMapper {
    List<Tag> getAllTags(@Param("plantCodes") List<String> plantCodes);
}