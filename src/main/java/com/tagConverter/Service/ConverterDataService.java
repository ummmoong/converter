package com.tagConverter.Service;

import com.tagConverter.mapper.Tag;
import com.tagConverter.mapper.TagMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * 데이터 조회 서비스 클래스
 * @author 스마트안전사업부 봉민석
 *
 */
@Service
public class ConverterDataService {
    @Autowired
    TagMapper tagMapper;
    public List<Tag> selectAllTags(List<String> plantCodes){ return tagMapper.getAllTags(plantCodes);}
}
