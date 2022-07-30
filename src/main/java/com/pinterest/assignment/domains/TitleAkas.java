package com.pinterest.assignment.domains;

/* created by Ali Tofigh  7/26/2022 12:57 AM */

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import javax.persistence.*;

@Entity
@Table(name = "title_akas")
@Setter
@Getter
@NoArgsConstructor
@AllArgsConstructor
public class TitleAkas {

    @Id
    @GeneratedValue
    private Long nConst;
    private String primaryName;
    private int birthYear;
    private int deathYear;
    private String primaryProfession;
    private String knownForTitles;
}
