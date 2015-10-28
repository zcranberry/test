/*
*目标表名： YYC.ACRM_ACRMBB_HXKHQXFXYBB_JGBNC(核心客户迁徙分析月报表_机构比年初)
*源    表： YWC.KHFX_KHPJBQL_KHPJPFXX_M(客户评级评分信息_月切片)
            ZBC.VIEW_KHHZ_KHZCFZYRJ_M(客户资产负债月日均_月切片)
            ZBC.HZZBC_KHHZ_KHZCFZYJS_M(客户资产负债月积数_月切片)
            YWC.KHFX_KHPJBQL_KHYWSX_M(客户业务属性_月切片)
            JCC.DSRZT_KHGSGXLSB_L(客户归属关系历史表)
            JCC.DSRZT_KHJBXX(客户基本信息)


*脚本名称： ACRM_ACRMBB_HXKHQXFXYBB_JGBNC.SQL
*AUTHOR  : 董晃
*创建日期：2014/06/04
*更改日期:(更改人+更改日期)：
*算    法：月报
*ETL策略 ：
*/

/*--  业务统计规则
1、总行系统管理员、零售总部管理岗、私人银行管理岗可以查看总行、指定分行或指定支行、辖内指定客户经理的报表；
2、分行主管可以查看本分行及下属指定支行、辖内指定客户经理的报表；支行主管可查看本支行、辖内指定客户经理报表；客户经理只能查看本人名下客户的统计报表（即只有一条记录）；
3、报表统计的时间范围是一个自然月；年初指去年12月末数据。
4、层级为客户当前考核层级；
新增口径为：与上月层级比较，当前层级比上月高的计为提升。提升按提升后的层级计。
例：A、B两位客户，A客户上月黄金，本月黑金，B客户上月白金，本月钻石，则本报表中A客户计为黑金提升1户，B客户计为钻石提升1户；
5、人数统计口径：比如客户上月层级为A，本月层级为B，则在A行B列记1户；
5、资产统计口径：资产均为月日均资产。统计规则为相应的统计人数中，本月资产较基期（上月或年初）的变动情况。增加为正数，减少用负数展示。其他类资产=行内总资产-储蓄-理财。
6、流失率销户率：销户客户数/上月XX层级客户数；
7、流失（销户）客户：统计周期（本月）内名下所有账户都销户；
8、流失挽回客户：仅指报表当月被评为流失挽回的客户。
即以上月为基数时，上月客户为无卡无账，本月为流失挽回客户。以年初为基数时，年初时点为无卡无账，但当前为流失挽回客户。
新开户客户，且半年内在我们有开立过账户（后销户）的客户；新增客户：基期无此客户的，作为新增客户统计。
9、本月各层级客户数合计数：上月层级为钻石、黑金、白金、黄金、普通、本月新增客户的合计数。

*/
/*
KHCJDM  客户层级代码  98  白金
KHCJDM  客户层级代码  97  黄金
KHCJDM  客户层级代码  96  一般核心
KHCJDM  客户层级代码  00  普通
KHCJDM  客户层级代码  95  钻石
KHCJDM  客户层级代码  99  黑金

QXKHCJDM  迁徙客户层级代码  98  白金
QXKHCJDM  迁徙客户层级代码  97  黄金
QXKHCJDM  迁徙客户层级代码  96  一般核心
QXKHCJDM  迁徙客户层级代码  00  普通
QXKHCJDM  迁徙客户层级代码  95  钻石
QXKHCJDM  迁徙客户层级代码  99  黑金
QXKHCJDM  迁徙客户层级代码  11  新增客户
QXKHCJDM  迁徙客户层级代码  12  流失挽回客户

*/


-- 设置上期日期 (上个月/去年)  本脚本设置为比上月
SET V_SQRQ = VALUES(SUBSTR(ADD_MONTHS(DATE '$ETL_DATE',-12),1,4)||'1231');
;

---创建临时表1  存放客户的属性信息
--CREATE TEMPORARY TABLE TMP1_ACRM_ACRMBB_HXKHQXFXYBB_JGBNC AS SELECT * FROM YYC.ACRM_ACRMBB_HXKHQXFXYBB_JGBNC WHERE 1=0;
CREATE TEMPORARY TABLE TMP1_ACRM_ACRMBB_HXKHQXFXYBB_JGBNC
(
        KHH             VARCHAR(32),           -- 客户号
        SQKHCJ          VARCHAR(2),            -- 上期客户层级
        BQKHCJ          VARCHAR(2),            -- 本期客户层级
        BQXHBZ          VARCHAR(1),            -- 本期销户标志
        SFBQXZKH        VARCHAR(1),            -- 是否本期新增客户
        SFBQLSWHKH      VARCHAR(1)             -- 是否本期流失挽回客户

)
DISTRIBUTED BY (KHH)
;

--装入临时表1
INSERT INTO TMP1_ACRM_ACRMBB_HXKHQXFXYBB_JGBNC
(
        KHH                                              -- 客户号
        ,SQKHCJ                                          -- 上期客户层级
        ,BQKHCJ                                          -- 本期客户层级
        ,BQXHBZ                                          -- 本期销户标志
        ,SFBQXZKH                                        -- 是否本期新增客户
        ,SFBQLSWHKH                                      -- 是否本期流失挽回客户
)
SELECT
        A.KHH                                               -- 客户号
       ,A.DYKHCJDM                                          -- 上期客户层级   @20150723 msz KHKHCJDM-客户考核层级代码 修改为 DYKHCJDM-当月考核层级代码
       ,B.DYKHCJDM                                          -- 本期客户层级   @20150723 msz KHKHCJDM-客户考核层级代码 修改为 DYKHCJDM-当月考核层级代码
       ,CASE WHEN J.XHRQ>DATE '$V_SQRQ' AND J.XHRQ<= DATE '$ETL_DATE' THEN '1'    --本月销户的客客户
             WHEN C.KHLSRQ>DATE '$V_SQRQ' AND C.KHLSRQ<= DATE '$ETL_DATE' THEN '1' --本月成为无卡无账的客户
             ELSE '0' END                              -- 本期销户标志
       ,CASE WHEN J.KHRQ>DATE '$V_SQRQ' AND J.KHRQ<= DATE '$ETL_DATE' AND J.XHRQ=DATE '3000-12-31' THEN '1' ELSE '0' END -- 是否本期新增客户
       ,CASE WHEN C.KHWHRQ>DATE '$V_SQRQ' AND C.KHWHRQ<= DATE '$ETL_DATE' THEN '1' ELSE '0' END                          -- 是否本期流失挽回客户
FROM   JCC.DSRZT_KHJBXX              J  --(客户基本信息)
INNER  JOIN
       YWC.KHFX_KHPJBQL_KHPJPFXX_M   A  --(客户评级评分信息_月切片)
ON     J.KHH=A.KHH
AND    A.YWRQ=DATE '$V_SQRQ'
LEFT   JOIN
       YWC.KHFX_KHPJBQL_KHPJPFXX_M   B  --(客户评级评分信息_月切片)
ON    J.KHH=B.KHH
AND    B.YWRQ=DATE '$ETL_DATE'
LEFT   JOIN
       YWC.KHFX_KHPJBQL_KHYWSX_M     C  --(客户业务属性_月切片)
ON     J.KHH=C.KHH
AND    A.KHH=C.KHH
--AND    B.KHH=C.KHH
AND    C.YWRQ=DATE '$ETL_DATE'
;


--装入目标表
DELETE FROM YYC.ACRM_ACRMBB_HXKHQXFXYBB_JGBNC WHERE YWRQ=DATE '$ETL_DATE';
INSERT INTO YYC.ACRM_ACRMBB_HXKHQXFXYBB_JGBNC
(
        JGH                                           -- 机构号
        ,NCKHKHCJDM                                   -- 年初客户考核层级代码
        ,YWRQ                                         -- 业务日期
        ,BYZSKHS                                      -- 本月钻石客户数
        ,BYHJKHS                                      -- 本月黑金客户数
        ,BYBJKHS                                      -- 本月白金客户数
        ,BYHJKHS1                                     -- 本月黄金客户数
        ,BYYBHXKHS                                    -- 本月一般核心客户数
        ,BYPTKHS                                      -- 本月普通客户数
        ,BYLSKHS                                      -- 本月流失客户数
        ,KHS                                          -- 客户数
        ,BYZSKHXNZZCYRJYE                             -- 本月钻石客户行内总资产月日均余额
        ,BYHJKHXNZZCYRJYE                             -- 本月黑金客户行内总资产月日均余额
        ,BYBJKHXNZZCYRJYE                             -- 本月白金客户行内总资产月日均余额
        ,BYHJKHXNZZCYRJYE1                            -- 本月黄金客户行内总资产月日均余额
        ,BYYBHXKHXNZZCYRJYE                           -- 本月一般核心客户行内总资产月日均余额
        ,BYPTKHXNZZCYRJYE                             -- 本月普通客户行内总资产月日均余额
        ,BYLSKHXNZZCYRJYE                             -- 本月流失客户行内总资产月日均余额
        ,BYZSKHCXYRJYE                                -- 本月钻石客户储蓄月日均余额
        ,BYHJKHCXYRJYE                                -- 本月黑金客户储蓄月日均余额
        ,BYBJKHCXYRJYE                                -- 本月白金客户储蓄月日均余额
        ,BYHJKHCXYRJYE1                               -- 本月黄金客户储蓄月日均余额
        ,BYYBHXKHCXYRJYE                              -- 本月一般核心客户储蓄月日均余额
        ,BYPTKHCXYRJYE                                -- 本月普通客户储蓄月日均余额
        ,BYLSKHCXYRJYE                                -- 本月流失客户储蓄月日均余额
        ,BYZSKHLCYRJYE                                -- 本月钻石客户理财月日均余额
        ,BYHJKHLCYRJYE                                -- 本月黑金客户理财月日均余额
        ,BYBJKHLCYRJYE                                -- 本月白金客户理财月日均余额
        ,BYHJKHLCYRJYE1                               -- 本月黄金客户理财月日均余额
        ,BYYBHXKHLCYRJYE                              -- 本月一般核心客户理财月日均余额
        ,BYPTKHLCYRJYE                                -- 本月普通客户理财月日均余额
        ,BYLSKHLCYRJYE                                -- 本月流失客户理财月日均余额
        ,BYZSKHQTZCYRJYE                              -- 本月钻石客户其他资产月日均余额
        ,BYHJKHQTZCYRJYE                              -- 本月黑金客户其他资产月日均余额
        ,BYBJKHQTZCYRJYE                              -- 本月白金客户其他资产月日均余额
        ,BYHJKHQTZCYRJYE1                             -- 本月黄金客户其他资产月日均余额
        ,BYYBHXKHQTZCYRJYE                            -- 本月一般核心客户其他资产月日均余额
        ,BYPTKHQTZCYRJYE                              -- 本月普通客户其他资产月日均余额
        ,BYLSKHQTZCYRJYE                              -- 本月流失客户其他资产月日均余额
)
SELECT
        A.GSZH
        ,TMP1.SQKHCJ
        ,DATE '$ETL_DATE'
        ,SUM(CASE WHEN TMP1.BQKHCJ='95' THEN 1 ELSE 0 END )                                                         -- 本月钻石客户数
        ,SUM(CASE WHEN TMP1.BQKHCJ='99' THEN 1 ELSE 0 END )                                                         -- 本月黑金客户数
        ,SUM(CASE WHEN TMP1.BQKHCJ='98' THEN 1 ELSE 0 END )                                                         -- 本月白金客户数
        ,SUM(CASE WHEN TMP1.BQKHCJ='97' THEN 1 ELSE 0 END )                                                         -- 本月黄金客户数
        ,SUM(CASE WHEN TMP1.BQKHCJ='96' THEN 1 ELSE 0 END )                                                         -- 本月一般核心客户数
        ,SUM(CASE WHEN TMP1.BQKHCJ='00' THEN 1 ELSE 0 END )                                                         -- 本月普通客户数
        ,SUM(CASE WHEN TMP1.BQXHBZ='1'  THEN 1 ELSE 0 END )                                                         -- 本月流失客户数
        ,COUNT(1)                                                                                           -- 客户数

        ,SUM(CASE WHEN TMP1.BQKHCJ='95' THEN COALESCE(B.HNZZCYRJ,0)-COALESCE(C.HNZZCYRJ,0) ELSE 0 END )                                    -- 本月钻石客户行内总资产月日均余额
        ,SUM(CASE WHEN TMP1.BQKHCJ='99' THEN COALESCE(B.HNZZCYRJ,0)-COALESCE(C.HNZZCYRJ,0) ELSE 0 END )                                    -- 本月黑金客户行内总资产月日均余额
        ,SUM(CASE WHEN TMP1.BQKHCJ='98' THEN COALESCE(B.HNZZCYRJ,0)-COALESCE(C.HNZZCYRJ,0) ELSE 0 END )                                    -- 本月白金客户行内总资产月日均余额
        ,SUM(CASE WHEN TMP1.BQKHCJ='97' THEN COALESCE(B.HNZZCYRJ,0)-COALESCE(C.HNZZCYRJ,0) ELSE 0 END )                                    -- 本月黄金客户行内总资产月日均余额
        ,SUM(CASE WHEN TMP1.BQKHCJ='96' THEN COALESCE(B.HNZZCYRJ,0)-COALESCE(C.HNZZCYRJ,0) ELSE 0 END )                                    -- 本月一般核心客户行内总资产月日均余额
        ,SUM(CASE WHEN TMP1.BQKHCJ='00' THEN COALESCE(B.HNZZCYRJ,0)-COALESCE(C.HNZZCYRJ,0) ELSE 0 END )                                    -- 本月普通客户行内总资产月日均余额
        ,SUM(CASE WHEN TMP1.BQXHBZ='1'  THEN COALESCE(B.HNZZCYRJ,0)-COALESCE(C.HNZZCYRJ,0) ELSE 0 END )                                    -- 本月流失客户行内总资产月日均余额

        ,SUM(CASE WHEN TMP1.BQKHCJ='95' THEN COALESCE(B.RMBCKYRJ,0)-COALESCE(C.RMBCKYRJ,0) ELSE 0 END )                                    -- 本月钻石客户储蓄月日均余额
        ,SUM(CASE WHEN TMP1.BQKHCJ='99' THEN COALESCE(B.RMBCKYRJ,0)-COALESCE(C.RMBCKYRJ,0) ELSE 0 END )                                    -- 本月黑金客户储蓄月日均余额
        ,SUM(CASE WHEN TMP1.BQKHCJ='98' THEN COALESCE(B.RMBCKYRJ,0)-COALESCE(C.RMBCKYRJ,0) ELSE 0 END )                                    -- 本月白金客户储蓄月日均余额
        ,SUM(CASE WHEN TMP1.BQKHCJ='97' THEN COALESCE(B.RMBCKYRJ,0)-COALESCE(C.RMBCKYRJ,0) ELSE 0 END )                                    -- 本月黄金客户储蓄月日均余额
        ,SUM(CASE WHEN TMP1.BQKHCJ='96' THEN COALESCE(B.RMBCKYRJ,0)-COALESCE(C.RMBCKYRJ,0) ELSE 0 END )                                    -- 本月一般核心客户储蓄月日均余额
        ,SUM(CASE WHEN TMP1.BQKHCJ='00' THEN COALESCE(B.RMBCKYRJ,0)-COALESCE(C.RMBCKYRJ,0) ELSE 0 END )                                    -- 本月普通客户储蓄月日均余额
        ,SUM(CASE WHEN TMP1.BQXHBZ='1'  THEN COALESCE(B.RMBCKYRJ,0)-COALESCE(C.RMBCKYRJ,0) ELSE 0 END )                                    -- 本月流失客户储蓄月日均余额

        ,SUM(CASE WHEN TMP1.BQKHCJ='95' THEN COALESCE(B.RMBLCYRJ,0)-COALESCE(C.RMBLCYRJ,0) ELSE 0 END )                                    --本月钻石客户理财月日均余额
        ,SUM(CASE WHEN TMP1.BQKHCJ='99' THEN COALESCE(B.RMBLCYRJ,0)-COALESCE(C.RMBLCYRJ,0) ELSE 0 END )                                    --本月黑金客户理财月日均余额
        ,SUM(CASE WHEN TMP1.BQKHCJ='98' THEN COALESCE(B.RMBLCYRJ,0)-COALESCE(C.RMBLCYRJ,0) ELSE 0 END )                                    --本月白金客户理财月日均余额
        ,SUM(CASE WHEN TMP1.BQKHCJ='97' THEN COALESCE(B.RMBLCYRJ,0)-COALESCE(C.RMBLCYRJ,0) ELSE 0 END )                                    --本月黄金客户理财月日均余额
        ,SUM(CASE WHEN TMP1.BQKHCJ='96' THEN COALESCE(B.RMBLCYRJ,0)-COALESCE(C.RMBLCYRJ,0) ELSE 0 END )                                    --本月一般核心客户理财月日均余额
        ,SUM(CASE WHEN TMP1.BQKHCJ='00' THEN COALESCE(B.RMBLCYRJ,0)-COALESCE(C.RMBLCYRJ,0) ELSE 0 END )                                    --本月普通客户理财月日均余额
        ,SUM(CASE WHEN TMP1.BQXHBZ='1'  THEN COALESCE(B.RMBLCYRJ,0)-COALESCE(C.RMBLCYRJ,0) ELSE 0 END )                                    --本月流失客户理财月日均余额

        ,SUM(CASE WHEN TMP1.BQKHCJ='95' THEN (COALESCE(B.HNZZCYRJ,0)-COALESCE(B.RMBCKYRJ,0)-COALESCE(B.RMBLCYRJ,0))-(COALESCE(C.HNZZCYRJ,0)-COALESCE(C.RMBCKYRJ,0)-COALESCE(C.RMBLCYRJ,0)) ELSE 0 END )      -- 本月钻石客户其他资产月日均余额
        ,SUM(CASE WHEN TMP1.BQKHCJ='99' THEN (COALESCE(B.HNZZCYRJ,0)-COALESCE(B.RMBCKYRJ,0)-COALESCE(B.RMBLCYRJ,0))-(COALESCE(C.HNZZCYRJ,0)-COALESCE(C.RMBCKYRJ,0)-COALESCE(C.RMBLCYRJ,0)) ELSE 0 END )      -- 本月黑金客户其他资产月日均余额
        ,SUM(CASE WHEN TMP1.BQKHCJ='98' THEN (COALESCE(B.HNZZCYRJ,0)-COALESCE(B.RMBCKYRJ,0)-COALESCE(B.RMBLCYRJ,0))-(COALESCE(C.HNZZCYRJ,0)-COALESCE(C.RMBCKYRJ,0)-COALESCE(C.RMBLCYRJ,0)) ELSE 0 END )      -- 本月白金客户其他资产月日均余额
        ,SUM(CASE WHEN TMP1.BQKHCJ='97' THEN (COALESCE(B.HNZZCYRJ,0)-COALESCE(B.RMBCKYRJ,0)-COALESCE(B.RMBLCYRJ,0))-(COALESCE(C.HNZZCYRJ,0)-COALESCE(C.RMBCKYRJ,0)-COALESCE(C.RMBLCYRJ,0)) ELSE 0 END )      -- 本月黄金客户其他资产月日均余额
        ,SUM(CASE WHEN TMP1.BQKHCJ='96' THEN (COALESCE(B.HNZZCYRJ,0)-COALESCE(B.RMBCKYRJ,0)-COALESCE(B.RMBLCYRJ,0))-(COALESCE(C.HNZZCYRJ,0)-COALESCE(C.RMBCKYRJ,0)-COALESCE(C.RMBLCYRJ,0)) ELSE 0 END )      -- 本月一般核心客户其他资产月日均余额
        ,SUM(CASE WHEN TMP1.BQKHCJ='00' THEN (COALESCE(B.HNZZCYRJ,0)-COALESCE(B.RMBCKYRJ,0)-COALESCE(B.RMBLCYRJ,0))-(COALESCE(C.HNZZCYRJ,0)-COALESCE(C.RMBCKYRJ,0)-COALESCE(C.RMBLCYRJ,0)) ELSE 0 END )      -- 本月普通客户其他资产月日均余额
        ,SUM(CASE WHEN TMP1.BQXHBZ='1'  THEN (COALESCE(B.HNZZCYRJ,0)-COALESCE(B.RMBCKYRJ,0)-COALESCE(B.RMBLCYRJ,0))-(COALESCE(C.HNZZCYRJ,0)-COALESCE(C.RMBCKYRJ,0)-COALESCE(C.RMBLCYRJ,0)) ELSE 0 END )      -- 本月流失客户其他资产月日均余额
FROM    TMP1_ACRM_ACRMBB_HXKHQXFXYBB_JGBNC  TMP1
INNER   JOIN
        JCC.DSRZT_KHGSGXLSB_L              A    -- (客户归属关系历史表)
ON      TMP1.KHH=A.KHH
AND     A.LLKSRQ <= DATE '$ETL_DATE'
AND     A.LLJSRQ > DATE '$ETL_DATE'
LEFT    JOIN
        ZBC.VIEW_KHHZ_KHZCFZYRJ_M          B    -- (客户资产负债月日均_月切片
ON      TMP1.KHH=B.KHH
AND     A.KHH=B.KHH
AND     B.YWRQ=DATE '$ETL_DATE'
LEFT    JOIN
        ZBC.VIEW_KHHZ_KHZCFZYRJ_M          C    -- (客户资产负债月日均_月切片
ON      TMP1.KHH=C.KHH
AND     A.KHH=C.KHH
AND     C.YWRQ=DATE '$V_SQRQ'
GROUP BY 1,2
;

--装入目标表，处理新增客户
INSERT INTO YYC.ACRM_ACRMBB_HXKHQXFXYBB_JGBNC
(
        JGH                                           -- 机构号
        ,NCKHKHCJDM                                   -- 年初客户考核层级代码
        ,YWRQ                                         -- 业务日期
        ,BYZSKHS                                      -- 本月钻石客户数
        ,BYHJKHS                                      -- 本月黑金客户数
        ,BYBJKHS                                      -- 本月白金客户数
        ,BYHJKHS1                                     -- 本月黄金客户数
        ,BYYBHXKHS                                    -- 本月一般核心客户数
        ,BYPTKHS                                      -- 本月普通客户数
        ,BYLSKHS                                      -- 本月流失客户数
        ,KHS                                          -- 客户数
        ,BYZSKHXNZZCYRJYE                             -- 本月钻石客户行内总资产月日均余额
        ,BYHJKHXNZZCYRJYE                             -- 本月黑金客户行内总资产月日均余额
        ,BYBJKHXNZZCYRJYE                             -- 本月白金客户行内总资产月日均余额
        ,BYHJKHXNZZCYRJYE1                            -- 本月黄金客户行内总资产月日均余额
        ,BYYBHXKHXNZZCYRJYE                           -- 本月一般核心客户行内总资产月日均余额
        ,BYPTKHXNZZCYRJYE                             -- 本月普通客户行内总资产月日均余额
        ,BYLSKHXNZZCYRJYE                             -- 本月流失客户行内总资产月日均余额
        ,BYZSKHCXYRJYE                                -- 本月钻石客户储蓄月日均余额
        ,BYHJKHCXYRJYE                                -- 本月黑金客户储蓄月日均余额
        ,BYBJKHCXYRJYE                                -- 本月白金客户储蓄月日均余额
        ,BYHJKHCXYRJYE1                               -- 本月黄金客户储蓄月日均余额
        ,BYYBHXKHCXYRJYE                              -- 本月一般核心客户储蓄月日均余额
        ,BYPTKHCXYRJYE                                -- 本月普通客户储蓄月日均余额
        ,BYLSKHCXYRJYE                                -- 本月流失客户储蓄月日均余额
        ,BYZSKHLCYRJYE                                -- 本月钻石客户理财月日均余额
        ,BYHJKHLCYRJYE                                -- 本月黑金客户理财月日均余额
        ,BYBJKHLCYRJYE                                -- 本月白金客户理财月日均余额
        ,BYHJKHLCYRJYE1                               -- 本月黄金客户理财月日均余额
        ,BYYBHXKHLCYRJYE                              -- 本月一般核心客户理财月日均余额
        ,BYPTKHLCYRJYE                                -- 本月普通客户理财月日均余额
        ,BYLSKHLCYRJYE                                -- 本月流失客户理财月日均余额
        ,BYZSKHQTZCYRJYE                              -- 本月钻石客户其他资产月日均余额
        ,BYHJKHQTZCYRJYE                              -- 本月黑金客户其他资产月日均余额
        ,BYBJKHQTZCYRJYE                              -- 本月白金客户其他资产月日均余额
        ,BYHJKHQTZCYRJYE1                             -- 本月黄金客户其他资产月日均余额
        ,BYYBHXKHQTZCYRJYE                            -- 本月一般核心客户其他资产月日均余额
        ,BYPTKHQTZCYRJYE                              -- 本月普通客户其他资产月日均余额
        ,BYLSKHQTZCYRJYE                              -- 本月流失客户其他资产月日均余额
)
SELECT
        A.GSZH
        ,'11'
        ,DATE '$ETL_DATE'
        ,SUM(CASE WHEN TMP1.BQKHCJ='95' THEN 1 ELSE 0 END )                                                         -- 本月钻石客户数
        ,SUM(CASE WHEN TMP1.BQKHCJ='99' THEN 1 ELSE 0 END )                                                         -- 本月黑金客户数
        ,SUM(CASE WHEN TMP1.BQKHCJ='98' THEN 1 ELSE 0 END )                                                         -- 本月白金客户数
        ,SUM(CASE WHEN TMP1.BQKHCJ='97' THEN 1 ELSE 0 END )                                                         -- 本月黄金客户数
        ,SUM(CASE WHEN TMP1.BQKHCJ='96' THEN 1 ELSE 0 END )                                                         -- 本月一般核心客户数
        ,SUM(CASE WHEN TMP1.BQKHCJ='00' THEN 1 ELSE 0 END )                                                         -- 本月普通客户数
        ,SUM(CASE WHEN TMP1.BQXHBZ='1'  THEN 1 ELSE 0 END )                                                         -- 本月流失客户数
        ,COUNT(1)                                                                                           -- 客户数

        ,SUM(CASE WHEN TMP1.BQKHCJ='95' THEN COALESCE(B.HNZZCYRJ,0)-COALESCE(C.HNZZCYRJ,0) ELSE 0 END )                                    -- 本月钻石客户行内总资产月日均余额
        ,SUM(CASE WHEN TMP1.BQKHCJ='99' THEN COALESCE(B.HNZZCYRJ,0)-COALESCE(C.HNZZCYRJ,0) ELSE 0 END )                                    -- 本月黑金客户行内总资产月日均余额
        ,SUM(CASE WHEN TMP1.BQKHCJ='98' THEN COALESCE(B.HNZZCYRJ,0)-COALESCE(C.HNZZCYRJ,0) ELSE 0 END )                                    -- 本月白金客户行内总资产月日均余额
        ,SUM(CASE WHEN TMP1.BQKHCJ='97' THEN COALESCE(B.HNZZCYRJ,0)-COALESCE(C.HNZZCYRJ,0) ELSE 0 END )                                    -- 本月黄金客户行内总资产月日均余额
        ,SUM(CASE WHEN TMP1.BQKHCJ='96' THEN COALESCE(B.HNZZCYRJ,0)-COALESCE(C.HNZZCYRJ,0) ELSE 0 END )                                    -- 本月一般核心客户行内总资产月日均余额
        ,SUM(CASE WHEN TMP1.BQKHCJ='00' THEN COALESCE(B.HNZZCYRJ,0)-COALESCE(C.HNZZCYRJ,0) ELSE 0 END )                                    -- 本月普通客户行内总资产月日均余额
        ,SUM(CASE WHEN TMP1.BQXHBZ='1'  THEN COALESCE(B.HNZZCYRJ,0)-COALESCE(C.HNZZCYRJ,0) ELSE 0 END )                                    -- 本月流失客户行内总资产月日均余额

        ,SUM(CASE WHEN TMP1.BQKHCJ='95' THEN COALESCE(B.RMBCKYRJ,0)-COALESCE(C.RMBCKYRJ,0) ELSE 0 END )                                    -- 本月钻石客户储蓄月日均余额
        ,SUM(CASE WHEN TMP1.BQKHCJ='99' THEN COALESCE(B.RMBCKYRJ,0)-COALESCE(C.RMBCKYRJ,0) ELSE 0 END )                                    -- 本月黑金客户储蓄月日均余额
        ,SUM(CASE WHEN TMP1.BQKHCJ='98' THEN COALESCE(B.RMBCKYRJ,0)-COALESCE(C.RMBCKYRJ,0) ELSE 0 END )                                    -- 本月白金客户储蓄月日均余额
        ,SUM(CASE WHEN TMP1.BQKHCJ='97' THEN COALESCE(B.RMBCKYRJ,0)-COALESCE(C.RMBCKYRJ,0) ELSE 0 END )                                    -- 本月黄金客户储蓄月日均余额
        ,SUM(CASE WHEN TMP1.BQKHCJ='96' THEN COALESCE(B.RMBCKYRJ,0)-COALESCE(C.RMBCKYRJ,0) ELSE 0 END )                                    -- 本月一般核心客户储蓄月日均余额
        ,SUM(CASE WHEN TMP1.BQKHCJ='00' THEN COALESCE(B.RMBCKYRJ,0)-COALESCE(C.RMBCKYRJ,0) ELSE 0 END )                                    -- 本月普通客户储蓄月日均余额
        ,SUM(CASE WHEN TMP1.BQXHBZ='1'  THEN COALESCE(B.RMBCKYRJ,0)-COALESCE(C.RMBCKYRJ,0) ELSE 0 END )                                    -- 本月流失客户储蓄月日均余额

        ,SUM(CASE WHEN TMP1.BQKHCJ='95' THEN COALESCE(B.RMBLCYRJ,0)-COALESCE(C.RMBLCYRJ,0) ELSE 0 END )                                    --本月钻石客户理财月日均余额
        ,SUM(CASE WHEN TMP1.BQKHCJ='99' THEN COALESCE(B.RMBLCYRJ,0)-COALESCE(C.RMBLCYRJ,0) ELSE 0 END )                                    --本月黑金客户理财月日均余额
        ,SUM(CASE WHEN TMP1.BQKHCJ='98' THEN COALESCE(B.RMBLCYRJ,0)-COALESCE(C.RMBLCYRJ,0) ELSE 0 END )                                    --本月白金客户理财月日均余额
        ,SUM(CASE WHEN TMP1.BQKHCJ='97' THEN COALESCE(B.RMBLCYRJ,0)-COALESCE(C.RMBLCYRJ,0) ELSE 0 END )                                    --本月黄金客户理财月日均余额
        ,SUM(CASE WHEN TMP1.BQKHCJ='96' THEN COALESCE(B.RMBLCYRJ,0)-COALESCE(C.RMBLCYRJ,0) ELSE 0 END )                                    --本月一般核心客户理财月日均余额
        ,SUM(CASE WHEN TMP1.BQKHCJ='00' THEN COALESCE(B.RMBLCYRJ,0)-COALESCE(C.RMBLCYRJ,0) ELSE 0 END )                                    --本月普通客户理财月日均余额
        ,SUM(CASE WHEN TMP1.BQXHBZ='1'  THEN COALESCE(B.RMBLCYRJ,0)-COALESCE(C.RMBLCYRJ,0) ELSE 0 END )                                    --本月流失客户理财月日均余额

        ,SUM(CASE WHEN TMP1.BQKHCJ='95' THEN (COALESCE(B.HNZZCYRJ,0)-COALESCE(B.RMBCKYRJ,0)-COALESCE(B.RMBLCYRJ,0))-(COALESCE(C.HNZZCYRJ,0)-COALESCE(C.RMBCKYRJ,0)-COALESCE(C.RMBLCYRJ,0)) ELSE 0 END )      -- 本月钻石客户其他资产月日均余额
        ,SUM(CASE WHEN TMP1.BQKHCJ='99' THEN (COALESCE(B.HNZZCYRJ,0)-COALESCE(B.RMBCKYRJ,0)-COALESCE(B.RMBLCYRJ,0))-(COALESCE(C.HNZZCYRJ,0)-COALESCE(C.RMBCKYRJ,0)-COALESCE(C.RMBLCYRJ,0)) ELSE 0 END )      -- 本月黑金客户其他资产月日均余额
        ,SUM(CASE WHEN TMP1.BQKHCJ='98' THEN (COALESCE(B.HNZZCYRJ,0)-COALESCE(B.RMBCKYRJ,0)-COALESCE(B.RMBLCYRJ,0))-(COALESCE(C.HNZZCYRJ,0)-COALESCE(C.RMBCKYRJ,0)-COALESCE(C.RMBLCYRJ,0)) ELSE 0 END )      -- 本月白金客户其他资产月日均余额
        ,SUM(CASE WHEN TMP1.BQKHCJ='97' THEN (COALESCE(B.HNZZCYRJ,0)-COALESCE(B.RMBCKYRJ,0)-COALESCE(B.RMBLCYRJ,0))-(COALESCE(C.HNZZCYRJ,0)-COALESCE(C.RMBCKYRJ,0)-COALESCE(C.RMBLCYRJ,0)) ELSE 0 END )      -- 本月黄金客户其他资产月日均余额
        ,SUM(CASE WHEN TMP1.BQKHCJ='96' THEN (COALESCE(B.HNZZCYRJ,0)-COALESCE(B.RMBCKYRJ,0)-COALESCE(B.RMBLCYRJ,0))-(COALESCE(C.HNZZCYRJ,0)-COALESCE(C.RMBCKYRJ,0)-COALESCE(C.RMBLCYRJ,0)) ELSE 0 END )      -- 本月一般核心客户其他资产月日均余额
        ,SUM(CASE WHEN TMP1.BQKHCJ='00' THEN (COALESCE(B.HNZZCYRJ,0)-COALESCE(B.RMBCKYRJ,0)-COALESCE(B.RMBLCYRJ,0))-(COALESCE(C.HNZZCYRJ,0)-COALESCE(C.RMBCKYRJ,0)-COALESCE(C.RMBLCYRJ,0)) ELSE 0 END )      -- 本月普通客户其他资产月日均余额
        ,SUM(CASE WHEN TMP1.BQXHBZ='1'  THEN (COALESCE(B.HNZZCYRJ,0)-COALESCE(B.RMBCKYRJ,0)-COALESCE(B.RMBLCYRJ,0))-(COALESCE(C.HNZZCYRJ,0)-COALESCE(C.RMBCKYRJ,0)-COALESCE(C.RMBLCYRJ,0)) ELSE 0 END )      -- 本月流失客户其他资产月日均余额
FROM    TMP1_ACRM_ACRMBB_HXKHQXFXYBB_JGBNC  TMP1
INNER   JOIN
        JCC.DSRZT_KHGSGXLSB_L              A    -- (客户归属关系历史表)
ON      TMP1.KHH=A.KHH
AND     A.LLKSRQ <= DATE '$ETL_DATE'
AND     A.LLJSRQ > DATE '$ETL_DATE'
LEFT    JOIN
        ZBC.VIEW_KHHZ_KHZCFZYRJ_M          B    -- (客户资产负债月日均_月切片
ON      TMP1.KHH=B.KHH
AND     A.KHH=B.KHH
AND     B.YWRQ=DATE '$ETL_DATE'
LEFT    JOIN
        ZBC.VIEW_KHHZ_KHZCFZYRJ_M          C    -- (客户资产负债月日均_月切片
ON      TMP1.KHH=C.KHH
AND     A.KHH=C.KHH
AND     C.YWRQ=DATE '$V_SQRQ'
WHERE TMP1.SFBQXZKH='1'--取本期新增客户
GROUP BY 1,2
;

--装入目标表，处理流失挽回客户
INSERT INTO YYC.ACRM_ACRMBB_HXKHQXFXYBB_JGBNC
(
        JGH                                           -- 机构号
        ,NCKHKHCJDM                                   -- 年初客户考核层级代码
        ,YWRQ                                         -- 业务日期
        ,BYZSKHS                                      -- 本月钻石客户数
        ,BYHJKHS                                      -- 本月黑金客户数
        ,BYBJKHS                                      -- 本月白金客户数
        ,BYHJKHS1                                     -- 本月黄金客户数
        ,BYYBHXKHS                                    -- 本月一般核心客户数
        ,BYPTKHS                                      -- 本月普通客户数
        ,BYLSKHS                                      -- 本月流失客户数
        ,KHS                                          -- 客户数
        ,BYZSKHXNZZCYRJYE                             -- 本月钻石客户行内总资产月日均余额
        ,BYHJKHXNZZCYRJYE                             -- 本月黑金客户行内总资产月日均余额
        ,BYBJKHXNZZCYRJYE                             -- 本月白金客户行内总资产月日均余额
        ,BYHJKHXNZZCYRJYE1                            -- 本月黄金客户行内总资产月日均余额
        ,BYYBHXKHXNZZCYRJYE                           -- 本月一般核心客户行内总资产月日均余额
        ,BYPTKHXNZZCYRJYE                             -- 本月普通客户行内总资产月日均余额
        ,BYLSKHXNZZCYRJYE                             -- 本月流失客户行内总资产月日均余额
        ,BYZSKHCXYRJYE                                -- 本月钻石客户储蓄月日均余额
        ,BYHJKHCXYRJYE                                -- 本月黑金客户储蓄月日均余额
        ,BYBJKHCXYRJYE                                -- 本月白金客户储蓄月日均余额
        ,BYHJKHCXYRJYE1                               -- 本月黄金客户储蓄月日均余额
        ,BYYBHXKHCXYRJYE                              -- 本月一般核心客户储蓄月日均余额
        ,BYPTKHCXYRJYE                                -- 本月普通客户储蓄月日均余额
        ,BYLSKHCXYRJYE                                -- 本月流失客户储蓄月日均余额
        ,BYZSKHLCYRJYE                                -- 本月钻石客户理财月日均余额
        ,BYHJKHLCYRJYE                                -- 本月黑金客户理财月日均余额
        ,BYBJKHLCYRJYE                                -- 本月白金客户理财月日均余额
        ,BYHJKHLCYRJYE1                               -- 本月黄金客户理财月日均余额
        ,BYYBHXKHLCYRJYE                              -- 本月一般核心客户理财月日均余额
        ,BYPTKHLCYRJYE                                -- 本月普通客户理财月日均余额
        ,BYLSKHLCYRJYE                                -- 本月流失客户理财月日均余额
        ,BYZSKHQTZCYRJYE                              -- 本月钻石客户其他资产月日均余额
        ,BYHJKHQTZCYRJYE                              -- 本月黑金客户其他资产月日均余额
        ,BYBJKHQTZCYRJYE                              -- 本月白金客户其他资产月日均余额
        ,BYHJKHQTZCYRJYE1                             -- 本月黄金客户其他资产月日均余额
        ,BYYBHXKHQTZCYRJYE                            -- 本月一般核心客户其他资产月日均余额
        ,BYPTKHQTZCYRJYE                              -- 本月普通客户其他资产月日均余额
        ,BYLSKHQTZCYRJYE                              -- 本月流失客户其他资产月日均余额
)
SELECT
        A.GSZH
        ,'12'
        ,DATE '$ETL_DATE'
        ,SUM(CASE WHEN TMP1.BQKHCJ='95' THEN 1 ELSE 0 END )                                                         -- 本月钻石客户数
        ,SUM(CASE WHEN TMP1.BQKHCJ='99' THEN 1 ELSE 0 END )                                                         -- 本月黑金客户数
        ,SUM(CASE WHEN TMP1.BQKHCJ='98' THEN 1 ELSE 0 END )                                                         -- 本月白金客户数
        ,SUM(CASE WHEN TMP1.BQKHCJ='97' THEN 1 ELSE 0 END )                                                         -- 本月黄金客户数
        ,SUM(CASE WHEN TMP1.BQKHCJ='96' THEN 1 ELSE 0 END )                                                         -- 本月一般核心客户数
        ,SUM(CASE WHEN TMP1.BQKHCJ='00' THEN 1 ELSE 0 END )                                                         -- 本月普通客户数
        ,SUM(CASE WHEN TMP1.BQXHBZ='1'  THEN 1 ELSE 0 END )                                                         -- 本月流失客户数
        ,COUNT(1)                                                                                           -- 客户数

        ,SUM(CASE WHEN TMP1.BQKHCJ='95' THEN COALESCE(B.HNZZCYRJ,0)-COALESCE(C.HNZZCYRJ,0) ELSE 0 END )                                    -- 本月钻石客户行内总资产月日均余额
        ,SUM(CASE WHEN TMP1.BQKHCJ='99' THEN COALESCE(B.HNZZCYRJ,0)-COALESCE(C.HNZZCYRJ,0) ELSE 0 END )                                    -- 本月黑金客户行内总资产月日均余额
        ,SUM(CASE WHEN TMP1.BQKHCJ='98' THEN COALESCE(B.HNZZCYRJ,0)-COALESCE(C.HNZZCYRJ,0) ELSE 0 END )                                    -- 本月白金客户行内总资产月日均余额
        ,SUM(CASE WHEN TMP1.BQKHCJ='97' THEN COALESCE(B.HNZZCYRJ,0)-COALESCE(C.HNZZCYRJ,0) ELSE 0 END )                                    -- 本月黄金客户行内总资产月日均余额
        ,SUM(CASE WHEN TMP1.BQKHCJ='96' THEN COALESCE(B.HNZZCYRJ,0)-COALESCE(C.HNZZCYRJ,0) ELSE 0 END )                                    -- 本月一般核心客户行内总资产月日均余额
        ,SUM(CASE WHEN TMP1.BQKHCJ='00' THEN COALESCE(B.HNZZCYRJ,0)-COALESCE(C.HNZZCYRJ,0) ELSE 0 END )                                    -- 本月普通客户行内总资产月日均余额
        ,SUM(CASE WHEN TMP1.BQXHBZ='1'  THEN COALESCE(B.HNZZCYRJ,0)-COALESCE(C.HNZZCYRJ,0) ELSE 0 END )                                    -- 本月流失客户行内总资产月日均余额

        ,SUM(CASE WHEN TMP1.BQKHCJ='95' THEN COALESCE(B.RMBCKYRJ,0)-COALESCE(C.RMBCKYRJ,0) ELSE 0 END )                                    -- 本月钻石客户储蓄月日均余额
        ,SUM(CASE WHEN TMP1.BQKHCJ='99' THEN COALESCE(B.RMBCKYRJ,0)-COALESCE(C.RMBCKYRJ,0) ELSE 0 END )                                    -- 本月黑金客户储蓄月日均余额
        ,SUM(CASE WHEN TMP1.BQKHCJ='98' THEN COALESCE(B.RMBCKYRJ,0)-COALESCE(C.RMBCKYRJ,0) ELSE 0 END )                                    -- 本月白金客户储蓄月日均余额
        ,SUM(CASE WHEN TMP1.BQKHCJ='97' THEN COALESCE(B.RMBCKYRJ,0)-COALESCE(C.RMBCKYRJ,0) ELSE 0 END )                                    -- 本月黄金客户储蓄月日均余额
        ,SUM(CASE WHEN TMP1.BQKHCJ='96' THEN COALESCE(B.RMBCKYRJ,0)-COALESCE(C.RMBCKYRJ,0) ELSE 0 END )                                    -- 本月一般核心客户储蓄月日均余额
        ,SUM(CASE WHEN TMP1.BQKHCJ='00' THEN COALESCE(B.RMBCKYRJ,0)-COALESCE(C.RMBCKYRJ,0) ELSE 0 END )                                    -- 本月普通客户储蓄月日均余额
        ,SUM(CASE WHEN TMP1.BQXHBZ='1'  THEN COALESCE(B.RMBCKYRJ,0)-COALESCE(C.RMBCKYRJ,0) ELSE 0 END )                                    -- 本月流失客户储蓄月日均余额

        ,SUM(CASE WHEN TMP1.BQKHCJ='95' THEN COALESCE(B.RMBLCYRJ,0)-COALESCE(C.RMBLCYRJ,0) ELSE 0 END )                                    --本月钻石客户理财月日均余额
        ,SUM(CASE WHEN TMP1.BQKHCJ='99' THEN COALESCE(B.RMBLCYRJ,0)-COALESCE(C.RMBLCYRJ,0) ELSE 0 END )                                    --本月黑金客户理财月日均余额
        ,SUM(CASE WHEN TMP1.BQKHCJ='98' THEN COALESCE(B.RMBLCYRJ,0)-COALESCE(C.RMBLCYRJ,0) ELSE 0 END )                                    --本月白金客户理财月日均余额
        ,SUM(CASE WHEN TMP1.BQKHCJ='97' THEN COALESCE(B.RMBLCYRJ,0)-COALESCE(C.RMBLCYRJ,0) ELSE 0 END )                                    --本月黄金客户理财月日均余额
        ,SUM(CASE WHEN TMP1.BQKHCJ='96' THEN COALESCE(B.RMBLCYRJ,0)-COALESCE(C.RMBLCYRJ,0) ELSE 0 END )                                    --本月一般核心客户理财月日均余额
        ,SUM(CASE WHEN TMP1.BQKHCJ='00' THEN COALESCE(B.RMBLCYRJ,0)-COALESCE(C.RMBLCYRJ,0) ELSE 0 END )                                    --本月普通客户理财月日均余额
        ,SUM(CASE WHEN TMP1.BQXHBZ='1'  THEN COALESCE(B.RMBLCYRJ,0)-COALESCE(C.RMBLCYRJ,0) ELSE 0 END )                                    --本月流失客户理财月日均余额

        ,SUM(CASE WHEN TMP1.BQKHCJ='95' THEN (COALESCE(B.HNZZCYRJ,0)-COALESCE(B.RMBCKYRJ,0)-COALESCE(B.RMBLCYRJ,0))-(COALESCE(C.HNZZCYRJ,0)-COALESCE(C.RMBCKYRJ,0)-COALESCE(C.RMBLCYRJ,0)) ELSE 0 END )      -- 本月钻石客户其他资产月日均余额
        ,SUM(CASE WHEN TMP1.BQKHCJ='99' THEN (COALESCE(B.HNZZCYRJ,0)-COALESCE(B.RMBCKYRJ,0)-COALESCE(B.RMBLCYRJ,0))-(COALESCE(C.HNZZCYRJ,0)-COALESCE(C.RMBCKYRJ,0)-COALESCE(C.RMBLCYRJ,0)) ELSE 0 END )      -- 本月黑金客户其他资产月日均余额
        ,SUM(CASE WHEN TMP1.BQKHCJ='98' THEN (COALESCE(B.HNZZCYRJ,0)-COALESCE(B.RMBCKYRJ,0)-COALESCE(B.RMBLCYRJ,0))-(COALESCE(C.HNZZCYRJ,0)-COALESCE(C.RMBCKYRJ,0)-COALESCE(C.RMBLCYRJ,0)) ELSE 0 END )      -- 本月白金客户其他资产月日均余额
        ,SUM(CASE WHEN TMP1.BQKHCJ='97' THEN (COALESCE(B.HNZZCYRJ,0)-COALESCE(B.RMBCKYRJ,0)-COALESCE(B.RMBLCYRJ,0))-(COALESCE(C.HNZZCYRJ,0)-COALESCE(C.RMBCKYRJ,0)-COALESCE(C.RMBLCYRJ,0)) ELSE 0 END )      -- 本月黄金客户其他资产月日均余额
        ,SUM(CASE WHEN TMP1.BQKHCJ='96' THEN (COALESCE(B.HNZZCYRJ,0)-COALESCE(B.RMBCKYRJ,0)-COALESCE(B.RMBLCYRJ,0))-(COALESCE(C.HNZZCYRJ,0)-COALESCE(C.RMBCKYRJ,0)-COALESCE(C.RMBLCYRJ,0)) ELSE 0 END )      -- 本月一般核心客户其他资产月日均余额
        ,SUM(CASE WHEN TMP1.BQKHCJ='00' THEN (COALESCE(B.HNZZCYRJ,0)-COALESCE(B.RMBCKYRJ,0)-COALESCE(B.RMBLCYRJ,0))-(COALESCE(C.HNZZCYRJ,0)-COALESCE(C.RMBCKYRJ,0)-COALESCE(C.RMBLCYRJ,0)) ELSE 0 END )      -- 本月普通客户其他资产月日均余额
        ,SUM(CASE WHEN TMP1.BQXHBZ='1'  THEN (COALESCE(B.HNZZCYRJ,0)-COALESCE(B.RMBCKYRJ,0)-COALESCE(B.RMBLCYRJ,0))-(COALESCE(C.HNZZCYRJ,0)-COALESCE(C.RMBCKYRJ,0)-COALESCE(C.RMBLCYRJ,0)) ELSE 0 END )      -- 本月流失客户其他资产月日均余额
FROM    TMP1_ACRM_ACRMBB_HXKHQXFXYBB_JGBNC  TMP1
INNER   JOIN
        JCC.DSRZT_KHGSGXLSB_L              A    -- (客户归属关系历史表)
ON      TMP1.KHH=A.KHH
AND     A.LLKSRQ <= DATE '$ETL_DATE'
AND     A.LLJSRQ > DATE '$ETL_DATE'
LEFT    JOIN
        ZBC.VIEW_KHHZ_KHZCFZYRJ_M          B    -- (客户资产负债月日均_月切片
ON      TMP1.KHH=B.KHH
AND     A.KHH=B.KHH
AND     B.YWRQ=DATE '$ETL_DATE'
LEFT    JOIN
        ZBC.VIEW_KHHZ_KHZCFZYRJ_M          C    -- (客户资产负债月日均_月切片
ON      TMP1.KHH=C.KHH
AND     A.KHH=C.KHH
AND     C.YWRQ=DATE '$V_SQRQ'
WHERE TMP1.SFBQLSWHKH='1' --取流失挽回客户
GROUP BY 1,2
;
COMMIT;