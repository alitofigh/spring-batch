package com.pinterest.assignment.helper;

/* created by Ali Tofigh  7/28/2022 1:18 AM */

import com.pinterest.assignment.domains.TitleAkas;
import org.springframework.batch.item.file.mapping.FieldSetMapper;
import org.springframework.batch.item.file.transform.FieldSet;
import org.springframework.validation.BindException;


public class TitleAkasMapper implements FieldSetMapper<TitleAkas> {

    @Override
    public TitleAkas mapFieldSet(FieldSet fieldSet) throws BindException {
        TitleAkas titleAkas = new TitleAkas();
        titleAkas.setNConst(Long.parseLong(fieldSet.readString(0).substring(2)));
        titleAkas.setPrimaryName(fieldSet.readString(1));
        titleAkas.setBirthYear(fieldSet.readString(2).equals("\\N") ? 0 : Integer.parseInt(fieldSet.readString(2)));
        titleAkas.setDeathYear(fieldSet.readString(3).equals("\\N") ? 0 : Integer.parseInt(fieldSet.readString(3)));
        titleAkas.setPrimaryProfession(fieldSet.readString(4).equals("\\N") ? null : fieldSet.readString(4));
        return titleAkas;
    }
}
