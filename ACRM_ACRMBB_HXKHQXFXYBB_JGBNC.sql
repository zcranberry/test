/*
*Ŀ������� YYC.ACRM_ACRMBB_HXKHQXFXYBB_JGBNC(���Ŀͻ�Ǩ������±���_���������)
*Դ    �� YWC.KHFX_KHPJBQL_KHPJPFXX_M(�ͻ�����������Ϣ_����Ƭ)
            ZBC.VIEW_KHHZ_KHZCFZYRJ_M(�ͻ��ʲ���ծ���վ�_����Ƭ)
            ZBC.HZZBC_KHHZ_KHZCFZYJS_M(�ͻ��ʲ���ծ�»���_����Ƭ)
            YWC.KHFX_KHPJBQL_KHYWSX_M(�ͻ�ҵ������_����Ƭ)
            JCC.DSRZT_KHGSGXLSB_L(�ͻ�������ϵ��ʷ��)
            JCC.DSRZT_KHJBXX(�ͻ�������Ϣ)


*�ű����ƣ� ACRM_ACRMBB_HXKHQXFXYBB_JGBNC.SQL
*AUTHOR  : ����
*�������ڣ�2014/06/04
*��������:(������+��������)��
*��    �����±�
*ETL���� ��
*/

/*--  ҵ��ͳ�ƹ���
1������ϵͳ����Ա�������ܲ�����ڡ�˽�����й���ڿ��Բ鿴���С�ָ�����л�ָ��֧�С�Ͻ��ָ���ͻ�����ı���
2���������ܿ��Բ鿴�����м�����ָ��֧�С�Ͻ��ָ���ͻ�����ı���֧�����ܿɲ鿴��֧�С�Ͻ��ָ���ͻ��������ͻ�����ֻ�ܲ鿴�������¿ͻ���ͳ�Ʊ�����ֻ��һ����¼����
3������ͳ�Ƶ�ʱ�䷶Χ��һ����Ȼ�£����ָȥ��12��ĩ���ݡ�
4���㼶Ϊ�ͻ���ǰ���˲㼶��
�����ھ�Ϊ�������²㼶�Ƚϣ���ǰ�㼶�����¸ߵļ�Ϊ������������������Ĳ㼶�ơ�
����A��B��λ�ͻ���A�ͻ����»ƽ𣬱��ºڽ�B�ͻ����°׽𣬱�����ʯ���򱾱�����A�ͻ���Ϊ�ڽ�����1����B�ͻ���Ϊ��ʯ����1����
5������ͳ�ƿھ�������ͻ����²㼶ΪA�����²㼶ΪB������A��B�м�1����
5���ʲ�ͳ�ƿھ����ʲ���Ϊ���վ��ʲ���ͳ�ƹ���Ϊ��Ӧ��ͳ�������У������ʲ��ϻ��ڣ����»�������ı䶯���������Ϊ�����������ø���չʾ���������ʲ�=�������ʲ�-����-��ơ�
6����ʧ�������ʣ������ͻ���/����XX�㼶�ͻ�����
7����ʧ���������ͻ���ͳ�����ڣ����£������������˻���������
8����ʧ��ؿͻ�����ָ�����±���Ϊ��ʧ��صĿͻ���
��������Ϊ����ʱ�����¿ͻ�Ϊ�޿����ˣ�����Ϊ��ʧ��ؿͻ��������Ϊ����ʱ�����ʱ��Ϊ�޿����ˣ�����ǰΪ��ʧ��ؿͻ���
�¿����ͻ����Ұ������������п������˻������������Ŀͻ��������ͻ��������޴˿ͻ��ģ���Ϊ�����ͻ�ͳ�ơ�
9�����¸��㼶�ͻ����ϼ��������²㼶Ϊ��ʯ���ڽ𡢰׽𡢻ƽ���ͨ�����������ͻ��ĺϼ�����

*/
/*
KHCJDM  �ͻ��㼶����  98  �׽�
KHCJDM  �ͻ��㼶����  97  �ƽ�
KHCJDM  �ͻ��㼶����  96  һ�����
KHCJDM  �ͻ��㼶����  00  ��ͨ
KHCJDM  �ͻ��㼶����  95  ��ʯ
KHCJDM  �ͻ��㼶����  99  �ڽ�

QXKHCJDM  Ǩ��ͻ��㼶����  98  �׽�
QXKHCJDM  Ǩ��ͻ��㼶����  97  �ƽ�
QXKHCJDM  Ǩ��ͻ��㼶����  96  һ�����
QXKHCJDM  Ǩ��ͻ��㼶����  00  ��ͨ
QXKHCJDM  Ǩ��ͻ��㼶����  95  ��ʯ
QXKHCJDM  Ǩ��ͻ��㼶����  99  �ڽ�
QXKHCJDM  Ǩ��ͻ��㼶����  11  �����ͻ�
QXKHCJDM  Ǩ��ͻ��㼶����  12  ��ʧ��ؿͻ�

*/


-- ������������ (�ϸ���/ȥ��)  ���ű�����Ϊ������
SET V_SQRQ = VALUES(SUBSTR(ADD_MONTHS(DATE '$ETL_DATE',-12),1,4)||'1231');
;

---������ʱ��1  ��ſͻ���������Ϣ
--CREATE TEMPORARY TABLE TMP1_ACRM_ACRMBB_HXKHQXFXYBB_JGBNC AS SELECT * FROM YYC.ACRM_ACRMBB_HXKHQXFXYBB_JGBNC WHERE 1=0;
CREATE TEMPORARY TABLE TMP1_ACRM_ACRMBB_HXKHQXFXYBB_JGBNC
(
        KHH             VARCHAR(32),           -- �ͻ���
        SQKHCJ          VARCHAR(2),            -- ���ڿͻ��㼶
        BQKHCJ          VARCHAR(2),            -- ���ڿͻ��㼶
        BQXHBZ          VARCHAR(1),            -- ����������־
        SFBQXZKH        VARCHAR(1),            -- �Ƿ��������ͻ�
        SFBQLSWHKH      VARCHAR(1)             -- �Ƿ�����ʧ��ؿͻ�

)
DISTRIBUTED BY (KHH)
;

--װ����ʱ��1
INSERT INTO TMP1_ACRM_ACRMBB_HXKHQXFXYBB_JGBNC
(
        KHH                                              -- �ͻ���
        ,SQKHCJ                                          -- ���ڿͻ��㼶
        ,BQKHCJ                                          -- ���ڿͻ��㼶
        ,BQXHBZ                                          -- ����������־
        ,SFBQXZKH                                        -- �Ƿ��������ͻ�
        ,SFBQLSWHKH                                      -- �Ƿ�����ʧ��ؿͻ�
)
SELECT
        A.KHH                                               -- �ͻ���
       ,A.DYKHCJDM                                          -- ���ڿͻ��㼶   @20150723 msz KHKHCJDM-�ͻ����˲㼶���� �޸�Ϊ DYKHCJDM-���¿��˲㼶����
       ,B.DYKHCJDM                                          -- ���ڿͻ��㼶   @20150723 msz KHKHCJDM-�ͻ����˲㼶���� �޸�Ϊ DYKHCJDM-���¿��˲㼶����
       ,CASE WHEN J.XHRQ>DATE '$V_SQRQ' AND J.XHRQ<= DATE '$ETL_DATE' THEN '1'    --���������ĿͿͻ�
             WHEN C.KHLSRQ>DATE '$V_SQRQ' AND C.KHLSRQ<= DATE '$ETL_DATE' THEN '1' --���³�Ϊ�޿����˵Ŀͻ�
             ELSE '0' END                              -- ����������־
       ,CASE WHEN J.KHRQ>DATE '$V_SQRQ' AND J.KHRQ<= DATE '$ETL_DATE' AND J.XHRQ=DATE '3000-12-31' THEN '1' ELSE '0' END -- �Ƿ��������ͻ�
       ,CASE WHEN C.KHWHRQ>DATE '$V_SQRQ' AND C.KHWHRQ<= DATE '$ETL_DATE' THEN '1' ELSE '0' END                          -- �Ƿ�����ʧ��ؿͻ�
FROM   JCC.DSRZT_KHJBXX              J  --(�ͻ�������Ϣ)
INNER  JOIN
       YWC.KHFX_KHPJBQL_KHPJPFXX_M   A  --(�ͻ�����������Ϣ_����Ƭ)
ON     J.KHH=A.KHH
AND    A.YWRQ=DATE '$V_SQRQ'
LEFT   JOIN
       YWC.KHFX_KHPJBQL_KHPJPFXX_M   B  --(�ͻ�����������Ϣ_����Ƭ)
ON    J.KHH=B.KHH
AND    B.YWRQ=DATE '$ETL_DATE'
LEFT   JOIN
       YWC.KHFX_KHPJBQL_KHYWSX_M     C  --(�ͻ�ҵ������_����Ƭ)
ON     J.KHH=C.KHH
AND    A.KHH=C.KHH
--AND    B.KHH=C.KHH
AND    C.YWRQ=DATE '$ETL_DATE'
;


--װ��Ŀ���
DELETE FROM YYC.ACRM_ACRMBB_HXKHQXFXYBB_JGBNC WHERE YWRQ=DATE '$ETL_DATE';
INSERT INTO YYC.ACRM_ACRMBB_HXKHQXFXYBB_JGBNC
(
        JGH                                           -- ������
        ,NCKHKHCJDM                                   -- ����ͻ����˲㼶����
        ,YWRQ                                         -- ҵ������
        ,BYZSKHS                                      -- ������ʯ�ͻ���
        ,BYHJKHS                                      -- ���ºڽ�ͻ���
        ,BYBJKHS                                      -- ���°׽�ͻ���
        ,BYHJKHS1                                     -- ���»ƽ�ͻ���
        ,BYYBHXKHS                                    -- ����һ����Ŀͻ���
        ,BYPTKHS                                      -- ������ͨ�ͻ���
        ,BYLSKHS                                      -- ������ʧ�ͻ���
        ,KHS                                          -- �ͻ���
        ,BYZSKHXNZZCYRJYE                             -- ������ʯ�ͻ��������ʲ����վ����
        ,BYHJKHXNZZCYRJYE                             -- ���ºڽ�ͻ��������ʲ����վ����
        ,BYBJKHXNZZCYRJYE                             -- ���°׽�ͻ��������ʲ����վ����
        ,BYHJKHXNZZCYRJYE1                            -- ���»ƽ�ͻ��������ʲ����վ����
        ,BYYBHXKHXNZZCYRJYE                           -- ����һ����Ŀͻ��������ʲ����վ����
        ,BYPTKHXNZZCYRJYE                             -- ������ͨ�ͻ��������ʲ����վ����
        ,BYLSKHXNZZCYRJYE                             -- ������ʧ�ͻ��������ʲ����վ����
        ,BYZSKHCXYRJYE                                -- ������ʯ�ͻ��������վ����
        ,BYHJKHCXYRJYE                                -- ���ºڽ�ͻ��������վ����
        ,BYBJKHCXYRJYE                                -- ���°׽�ͻ��������վ����
        ,BYHJKHCXYRJYE1                               -- ���»ƽ�ͻ��������վ����
        ,BYYBHXKHCXYRJYE                              -- ����һ����Ŀͻ��������վ����
        ,BYPTKHCXYRJYE                                -- ������ͨ�ͻ��������վ����
        ,BYLSKHCXYRJYE                                -- ������ʧ�ͻ��������վ����
        ,BYZSKHLCYRJYE                                -- ������ʯ�ͻ�������վ����
        ,BYHJKHLCYRJYE                                -- ���ºڽ�ͻ�������վ����
        ,BYBJKHLCYRJYE                                -- ���°׽�ͻ�������վ����
        ,BYHJKHLCYRJYE1                               -- ���»ƽ�ͻ�������վ����
        ,BYYBHXKHLCYRJYE                              -- ����һ����Ŀͻ�������վ����
        ,BYPTKHLCYRJYE                                -- ������ͨ�ͻ�������վ����
        ,BYLSKHLCYRJYE                                -- ������ʧ�ͻ�������վ����
        ,BYZSKHQTZCYRJYE                              -- ������ʯ�ͻ������ʲ����վ����
        ,BYHJKHQTZCYRJYE                              -- ���ºڽ�ͻ������ʲ����վ����
        ,BYBJKHQTZCYRJYE                              -- ���°׽�ͻ������ʲ����վ����
        ,BYHJKHQTZCYRJYE1                             -- ���»ƽ�ͻ������ʲ����վ����
        ,BYYBHXKHQTZCYRJYE                            -- ����һ����Ŀͻ������ʲ����վ����
        ,BYPTKHQTZCYRJYE                              -- ������ͨ�ͻ������ʲ����վ����
        ,BYLSKHQTZCYRJYE                              -- ������ʧ�ͻ������ʲ����վ����
)
SELECT
        A.GSZH
        ,TMP1.SQKHCJ
        ,DATE '$ETL_DATE'
        ,SUM(CASE WHEN TMP1.BQKHCJ='95' THEN 1 ELSE 0 END )                                                         -- ������ʯ�ͻ���
        ,SUM(CASE WHEN TMP1.BQKHCJ='99' THEN 1 ELSE 0 END )                                                         -- ���ºڽ�ͻ���
        ,SUM(CASE WHEN TMP1.BQKHCJ='98' THEN 1 ELSE 0 END )                                                         -- ���°׽�ͻ���
        ,SUM(CASE WHEN TMP1.BQKHCJ='97' THEN 1 ELSE 0 END )                                                         -- ���»ƽ�ͻ���
        ,SUM(CASE WHEN TMP1.BQKHCJ='96' THEN 1 ELSE 0 END )                                                         -- ����һ����Ŀͻ���
        ,SUM(CASE WHEN TMP1.BQKHCJ='00' THEN 1 ELSE 0 END )                                                         -- ������ͨ�ͻ���
        ,SUM(CASE WHEN TMP1.BQXHBZ='1'  THEN 1 ELSE 0 END )                                                         -- ������ʧ�ͻ���
        ,COUNT(1)                                                                                           -- �ͻ���

        ,SUM(CASE WHEN TMP1.BQKHCJ='95' THEN COALESCE(B.HNZZCYRJ,0)-COALESCE(C.HNZZCYRJ,0) ELSE 0 END )                                    -- ������ʯ�ͻ��������ʲ����վ����
        ,SUM(CASE WHEN TMP1.BQKHCJ='99' THEN COALESCE(B.HNZZCYRJ,0)-COALESCE(C.HNZZCYRJ,0) ELSE 0 END )                                    -- ���ºڽ�ͻ��������ʲ����վ����
        ,SUM(CASE WHEN TMP1.BQKHCJ='98' THEN COALESCE(B.HNZZCYRJ,0)-COALESCE(C.HNZZCYRJ,0) ELSE 0 END )                                    -- ���°׽�ͻ��������ʲ����վ����
        ,SUM(CASE WHEN TMP1.BQKHCJ='97' THEN COALESCE(B.HNZZCYRJ,0)-COALESCE(C.HNZZCYRJ,0) ELSE 0 END )                                    -- ���»ƽ�ͻ��������ʲ����վ����
        ,SUM(CASE WHEN TMP1.BQKHCJ='96' THEN COALESCE(B.HNZZCYRJ,0)-COALESCE(C.HNZZCYRJ,0) ELSE 0 END )                                    -- ����һ����Ŀͻ��������ʲ����վ����
        ,SUM(CASE WHEN TMP1.BQKHCJ='00' THEN COALESCE(B.HNZZCYRJ,0)-COALESCE(C.HNZZCYRJ,0) ELSE 0 END )                                    -- ������ͨ�ͻ��������ʲ����վ����
        ,SUM(CASE WHEN TMP1.BQXHBZ='1'  THEN COALESCE(B.HNZZCYRJ,0)-COALESCE(C.HNZZCYRJ,0) ELSE 0 END )                                    -- ������ʧ�ͻ��������ʲ����վ����

        ,SUM(CASE WHEN TMP1.BQKHCJ='95' THEN COALESCE(B.RMBCKYRJ,0)-COALESCE(C.RMBCKYRJ,0) ELSE 0 END )                                    -- ������ʯ�ͻ��������վ����
        ,SUM(CASE WHEN TMP1.BQKHCJ='99' THEN COALESCE(B.RMBCKYRJ,0)-COALESCE(C.RMBCKYRJ,0) ELSE 0 END )                                    -- ���ºڽ�ͻ��������վ����
        ,SUM(CASE WHEN TMP1.BQKHCJ='98' THEN COALESCE(B.RMBCKYRJ,0)-COALESCE(C.RMBCKYRJ,0) ELSE 0 END )                                    -- ���°׽�ͻ��������վ����
        ,SUM(CASE WHEN TMP1.BQKHCJ='97' THEN COALESCE(B.RMBCKYRJ,0)-COALESCE(C.RMBCKYRJ,0) ELSE 0 END )                                    -- ���»ƽ�ͻ��������վ����
        ,SUM(CASE WHEN TMP1.BQKHCJ='96' THEN COALESCE(B.RMBCKYRJ,0)-COALESCE(C.RMBCKYRJ,0) ELSE 0 END )                                    -- ����һ����Ŀͻ��������վ����
        ,SUM(CASE WHEN TMP1.BQKHCJ='00' THEN COALESCE(B.RMBCKYRJ,0)-COALESCE(C.RMBCKYRJ,0) ELSE 0 END )                                    -- ������ͨ�ͻ��������վ����
        ,SUM(CASE WHEN TMP1.BQXHBZ='1'  THEN COALESCE(B.RMBCKYRJ,0)-COALESCE(C.RMBCKYRJ,0) ELSE 0 END )                                    -- ������ʧ�ͻ��������վ����

        ,SUM(CASE WHEN TMP1.BQKHCJ='95' THEN COALESCE(B.RMBLCYRJ,0)-COALESCE(C.RMBLCYRJ,0) ELSE 0 END )                                    --������ʯ�ͻ�������վ����
        ,SUM(CASE WHEN TMP1.BQKHCJ='99' THEN COALESCE(B.RMBLCYRJ,0)-COALESCE(C.RMBLCYRJ,0) ELSE 0 END )                                    --���ºڽ�ͻ�������վ����
        ,SUM(CASE WHEN TMP1.BQKHCJ='98' THEN COALESCE(B.RMBLCYRJ,0)-COALESCE(C.RMBLCYRJ,0) ELSE 0 END )                                    --���°׽�ͻ�������վ����
        ,SUM(CASE WHEN TMP1.BQKHCJ='97' THEN COALESCE(B.RMBLCYRJ,0)-COALESCE(C.RMBLCYRJ,0) ELSE 0 END )                                    --���»ƽ�ͻ�������վ����
        ,SUM(CASE WHEN TMP1.BQKHCJ='96' THEN COALESCE(B.RMBLCYRJ,0)-COALESCE(C.RMBLCYRJ,0) ELSE 0 END )                                    --����һ����Ŀͻ�������վ����
        ,SUM(CASE WHEN TMP1.BQKHCJ='00' THEN COALESCE(B.RMBLCYRJ,0)-COALESCE(C.RMBLCYRJ,0) ELSE 0 END )                                    --������ͨ�ͻ�������վ����
        ,SUM(CASE WHEN TMP1.BQXHBZ='1'  THEN COALESCE(B.RMBLCYRJ,0)-COALESCE(C.RMBLCYRJ,0) ELSE 0 END )                                    --������ʧ�ͻ�������վ����

        ,SUM(CASE WHEN TMP1.BQKHCJ='95' THEN (COALESCE(B.HNZZCYRJ,0)-COALESCE(B.RMBCKYRJ,0)-COALESCE(B.RMBLCYRJ,0))-(COALESCE(C.HNZZCYRJ,0)-COALESCE(C.RMBCKYRJ,0)-COALESCE(C.RMBLCYRJ,0)) ELSE 0 END )      -- ������ʯ�ͻ������ʲ����վ����
        ,SUM(CASE WHEN TMP1.BQKHCJ='99' THEN (COALESCE(B.HNZZCYRJ,0)-COALESCE(B.RMBCKYRJ,0)-COALESCE(B.RMBLCYRJ,0))-(COALESCE(C.HNZZCYRJ,0)-COALESCE(C.RMBCKYRJ,0)-COALESCE(C.RMBLCYRJ,0)) ELSE 0 END )      -- ���ºڽ�ͻ������ʲ����վ����
        ,SUM(CASE WHEN TMP1.BQKHCJ='98' THEN (COALESCE(B.HNZZCYRJ,0)-COALESCE(B.RMBCKYRJ,0)-COALESCE(B.RMBLCYRJ,0))-(COALESCE(C.HNZZCYRJ,0)-COALESCE(C.RMBCKYRJ,0)-COALESCE(C.RMBLCYRJ,0)) ELSE 0 END )      -- ���°׽�ͻ������ʲ����վ����
        ,SUM(CASE WHEN TMP1.BQKHCJ='97' THEN (COALESCE(B.HNZZCYRJ,0)-COALESCE(B.RMBCKYRJ,0)-COALESCE(B.RMBLCYRJ,0))-(COALESCE(C.HNZZCYRJ,0)-COALESCE(C.RMBCKYRJ,0)-COALESCE(C.RMBLCYRJ,0)) ELSE 0 END )      -- ���»ƽ�ͻ������ʲ����վ����
        ,SUM(CASE WHEN TMP1.BQKHCJ='96' THEN (COALESCE(B.HNZZCYRJ,0)-COALESCE(B.RMBCKYRJ,0)-COALESCE(B.RMBLCYRJ,0))-(COALESCE(C.HNZZCYRJ,0)-COALESCE(C.RMBCKYRJ,0)-COALESCE(C.RMBLCYRJ,0)) ELSE 0 END )      -- ����һ����Ŀͻ������ʲ����վ����
        ,SUM(CASE WHEN TMP1.BQKHCJ='00' THEN (COALESCE(B.HNZZCYRJ,0)-COALESCE(B.RMBCKYRJ,0)-COALESCE(B.RMBLCYRJ,0))-(COALESCE(C.HNZZCYRJ,0)-COALESCE(C.RMBCKYRJ,0)-COALESCE(C.RMBLCYRJ,0)) ELSE 0 END )      -- ������ͨ�ͻ������ʲ����վ����
        ,SUM(CASE WHEN TMP1.BQXHBZ='1'  THEN (COALESCE(B.HNZZCYRJ,0)-COALESCE(B.RMBCKYRJ,0)-COALESCE(B.RMBLCYRJ,0))-(COALESCE(C.HNZZCYRJ,0)-COALESCE(C.RMBCKYRJ,0)-COALESCE(C.RMBLCYRJ,0)) ELSE 0 END )      -- ������ʧ�ͻ������ʲ����վ����
FROM    TMP1_ACRM_ACRMBB_HXKHQXFXYBB_JGBNC  TMP1
INNER   JOIN
        JCC.DSRZT_KHGSGXLSB_L              A    -- (�ͻ�������ϵ��ʷ��)
ON      TMP1.KHH=A.KHH
AND     A.LLKSRQ <= DATE '$ETL_DATE'
AND     A.LLJSRQ > DATE '$ETL_DATE'
LEFT    JOIN
        ZBC.VIEW_KHHZ_KHZCFZYRJ_M          B    -- (�ͻ��ʲ���ծ���վ�_����Ƭ
ON      TMP1.KHH=B.KHH
AND     A.KHH=B.KHH
AND     B.YWRQ=DATE '$ETL_DATE'
LEFT    JOIN
        ZBC.VIEW_KHHZ_KHZCFZYRJ_M          C    -- (�ͻ��ʲ���ծ���վ�_����Ƭ
ON      TMP1.KHH=C.KHH
AND     A.KHH=C.KHH
AND     C.YWRQ=DATE '$V_SQRQ'
GROUP BY 1,2
;

--װ��Ŀ������������ͻ�
INSERT INTO YYC.ACRM_ACRMBB_HXKHQXFXYBB_JGBNC
(
        JGH                                           -- ������
        ,NCKHKHCJDM                                   -- ����ͻ����˲㼶����
        ,YWRQ                                         -- ҵ������
        ,BYZSKHS                                      -- ������ʯ�ͻ���
        ,BYHJKHS                                      -- ���ºڽ�ͻ���
        ,BYBJKHS                                      -- ���°׽�ͻ���
        ,BYHJKHS1                                     -- ���»ƽ�ͻ���
        ,BYYBHXKHS                                    -- ����һ����Ŀͻ���
        ,BYPTKHS                                      -- ������ͨ�ͻ���
        ,BYLSKHS                                      -- ������ʧ�ͻ���
        ,KHS                                          -- �ͻ���
        ,BYZSKHXNZZCYRJYE                             -- ������ʯ�ͻ��������ʲ����վ����
        ,BYHJKHXNZZCYRJYE                             -- ���ºڽ�ͻ��������ʲ����վ����
        ,BYBJKHXNZZCYRJYE                             -- ���°׽�ͻ��������ʲ����վ����
        ,BYHJKHXNZZCYRJYE1                            -- ���»ƽ�ͻ��������ʲ����վ����
        ,BYYBHXKHXNZZCYRJYE                           -- ����һ����Ŀͻ��������ʲ����վ����
        ,BYPTKHXNZZCYRJYE                             -- ������ͨ�ͻ��������ʲ����վ����
        ,BYLSKHXNZZCYRJYE                             -- ������ʧ�ͻ��������ʲ����վ����
        ,BYZSKHCXYRJYE                                -- ������ʯ�ͻ��������վ����
        ,BYHJKHCXYRJYE                                -- ���ºڽ�ͻ��������վ����
        ,BYBJKHCXYRJYE                                -- ���°׽�ͻ��������վ����
        ,BYHJKHCXYRJYE1                               -- ���»ƽ�ͻ��������վ����
        ,BYYBHXKHCXYRJYE                              -- ����һ����Ŀͻ��������վ����
        ,BYPTKHCXYRJYE                                -- ������ͨ�ͻ��������վ����
        ,BYLSKHCXYRJYE                                -- ������ʧ�ͻ��������վ����
        ,BYZSKHLCYRJYE                                -- ������ʯ�ͻ�������վ����
        ,BYHJKHLCYRJYE                                -- ���ºڽ�ͻ�������վ����
        ,BYBJKHLCYRJYE                                -- ���°׽�ͻ�������վ����
        ,BYHJKHLCYRJYE1                               -- ���»ƽ�ͻ�������վ����
        ,BYYBHXKHLCYRJYE                              -- ����һ����Ŀͻ�������վ����
        ,BYPTKHLCYRJYE                                -- ������ͨ�ͻ�������վ����
        ,BYLSKHLCYRJYE                                -- ������ʧ�ͻ�������վ����
        ,BYZSKHQTZCYRJYE                              -- ������ʯ�ͻ������ʲ����վ����
        ,BYHJKHQTZCYRJYE                              -- ���ºڽ�ͻ������ʲ����վ����
        ,BYBJKHQTZCYRJYE                              -- ���°׽�ͻ������ʲ����վ����
        ,BYHJKHQTZCYRJYE1                             -- ���»ƽ�ͻ������ʲ����վ����
        ,BYYBHXKHQTZCYRJYE                            -- ����һ����Ŀͻ������ʲ����վ����
        ,BYPTKHQTZCYRJYE                              -- ������ͨ�ͻ������ʲ����վ����
        ,BYLSKHQTZCYRJYE                              -- ������ʧ�ͻ������ʲ����վ����
)
SELECT
        A.GSZH
        ,'11'
        ,DATE '$ETL_DATE'
        ,SUM(CASE WHEN TMP1.BQKHCJ='95' THEN 1 ELSE 0 END )                                                         -- ������ʯ�ͻ���
        ,SUM(CASE WHEN TMP1.BQKHCJ='99' THEN 1 ELSE 0 END )                                                         -- ���ºڽ�ͻ���
        ,SUM(CASE WHEN TMP1.BQKHCJ='98' THEN 1 ELSE 0 END )                                                         -- ���°׽�ͻ���
        ,SUM(CASE WHEN TMP1.BQKHCJ='97' THEN 1 ELSE 0 END )                                                         -- ���»ƽ�ͻ���
        ,SUM(CASE WHEN TMP1.BQKHCJ='96' THEN 1 ELSE 0 END )                                                         -- ����һ����Ŀͻ���
        ,SUM(CASE WHEN TMP1.BQKHCJ='00' THEN 1 ELSE 0 END )                                                         -- ������ͨ�ͻ���
        ,SUM(CASE WHEN TMP1.BQXHBZ='1'  THEN 1 ELSE 0 END )                                                         -- ������ʧ�ͻ���
        ,COUNT(1)                                                                                           -- �ͻ���

        ,SUM(CASE WHEN TMP1.BQKHCJ='95' THEN COALESCE(B.HNZZCYRJ,0)-COALESCE(C.HNZZCYRJ,0) ELSE 0 END )                                    -- ������ʯ�ͻ��������ʲ����վ����
        ,SUM(CASE WHEN TMP1.BQKHCJ='99' THEN COALESCE(B.HNZZCYRJ,0)-COALESCE(C.HNZZCYRJ,0) ELSE 0 END )                                    -- ���ºڽ�ͻ��������ʲ����վ����
        ,SUM(CASE WHEN TMP1.BQKHCJ='98' THEN COALESCE(B.HNZZCYRJ,0)-COALESCE(C.HNZZCYRJ,0) ELSE 0 END )                                    -- ���°׽�ͻ��������ʲ����վ����
        ,SUM(CASE WHEN TMP1.BQKHCJ='97' THEN COALESCE(B.HNZZCYRJ,0)-COALESCE(C.HNZZCYRJ,0) ELSE 0 END )                                    -- ���»ƽ�ͻ��������ʲ����վ����
        ,SUM(CASE WHEN TMP1.BQKHCJ='96' THEN COALESCE(B.HNZZCYRJ,0)-COALESCE(C.HNZZCYRJ,0) ELSE 0 END )                                    -- ����һ����Ŀͻ��������ʲ����վ����
        ,SUM(CASE WHEN TMP1.BQKHCJ='00' THEN COALESCE(B.HNZZCYRJ,0)-COALESCE(C.HNZZCYRJ,0) ELSE 0 END )                                    -- ������ͨ�ͻ��������ʲ����վ����
        ,SUM(CASE WHEN TMP1.BQXHBZ='1'  THEN COALESCE(B.HNZZCYRJ,0)-COALESCE(C.HNZZCYRJ,0) ELSE 0 END )                                    -- ������ʧ�ͻ��������ʲ����վ����

        ,SUM(CASE WHEN TMP1.BQKHCJ='95' THEN COALESCE(B.RMBCKYRJ,0)-COALESCE(C.RMBCKYRJ,0) ELSE 0 END )                                    -- ������ʯ�ͻ��������վ����
        ,SUM(CASE WHEN TMP1.BQKHCJ='99' THEN COALESCE(B.RMBCKYRJ,0)-COALESCE(C.RMBCKYRJ,0) ELSE 0 END )                                    -- ���ºڽ�ͻ��������վ����
        ,SUM(CASE WHEN TMP1.BQKHCJ='98' THEN COALESCE(B.RMBCKYRJ,0)-COALESCE(C.RMBCKYRJ,0) ELSE 0 END )                                    -- ���°׽�ͻ��������վ����
        ,SUM(CASE WHEN TMP1.BQKHCJ='97' THEN COALESCE(B.RMBCKYRJ,0)-COALESCE(C.RMBCKYRJ,0) ELSE 0 END )                                    -- ���»ƽ�ͻ��������վ����
        ,SUM(CASE WHEN TMP1.BQKHCJ='96' THEN COALESCE(B.RMBCKYRJ,0)-COALESCE(C.RMBCKYRJ,0) ELSE 0 END )                                    -- ����һ����Ŀͻ��������վ����
        ,SUM(CASE WHEN TMP1.BQKHCJ='00' THEN COALESCE(B.RMBCKYRJ,0)-COALESCE(C.RMBCKYRJ,0) ELSE 0 END )                                    -- ������ͨ�ͻ��������վ����
        ,SUM(CASE WHEN TMP1.BQXHBZ='1'  THEN COALESCE(B.RMBCKYRJ,0)-COALESCE(C.RMBCKYRJ,0) ELSE 0 END )                                    -- ������ʧ�ͻ��������վ����

        ,SUM(CASE WHEN TMP1.BQKHCJ='95' THEN COALESCE(B.RMBLCYRJ,0)-COALESCE(C.RMBLCYRJ,0) ELSE 0 END )                                    --������ʯ�ͻ�������վ����
        ,SUM(CASE WHEN TMP1.BQKHCJ='99' THEN COALESCE(B.RMBLCYRJ,0)-COALESCE(C.RMBLCYRJ,0) ELSE 0 END )                                    --���ºڽ�ͻ�������վ����
        ,SUM(CASE WHEN TMP1.BQKHCJ='98' THEN COALESCE(B.RMBLCYRJ,0)-COALESCE(C.RMBLCYRJ,0) ELSE 0 END )                                    --���°׽�ͻ�������վ����
        ,SUM(CASE WHEN TMP1.BQKHCJ='97' THEN COALESCE(B.RMBLCYRJ,0)-COALESCE(C.RMBLCYRJ,0) ELSE 0 END )                                    --���»ƽ�ͻ�������վ����
        ,SUM(CASE WHEN TMP1.BQKHCJ='96' THEN COALESCE(B.RMBLCYRJ,0)-COALESCE(C.RMBLCYRJ,0) ELSE 0 END )                                    --����һ����Ŀͻ�������վ����
        ,SUM(CASE WHEN TMP1.BQKHCJ='00' THEN COALESCE(B.RMBLCYRJ,0)-COALESCE(C.RMBLCYRJ,0) ELSE 0 END )                                    --������ͨ�ͻ�������վ����
        ,SUM(CASE WHEN TMP1.BQXHBZ='1'  THEN COALESCE(B.RMBLCYRJ,0)-COALESCE(C.RMBLCYRJ,0) ELSE 0 END )                                    --������ʧ�ͻ�������վ����

        ,SUM(CASE WHEN TMP1.BQKHCJ='95' THEN (COALESCE(B.HNZZCYRJ,0)-COALESCE(B.RMBCKYRJ,0)-COALESCE(B.RMBLCYRJ,0))-(COALESCE(C.HNZZCYRJ,0)-COALESCE(C.RMBCKYRJ,0)-COALESCE(C.RMBLCYRJ,0)) ELSE 0 END )      -- ������ʯ�ͻ������ʲ����վ����
        ,SUM(CASE WHEN TMP1.BQKHCJ='99' THEN (COALESCE(B.HNZZCYRJ,0)-COALESCE(B.RMBCKYRJ,0)-COALESCE(B.RMBLCYRJ,0))-(COALESCE(C.HNZZCYRJ,0)-COALESCE(C.RMBCKYRJ,0)-COALESCE(C.RMBLCYRJ,0)) ELSE 0 END )      -- ���ºڽ�ͻ������ʲ����վ����
        ,SUM(CASE WHEN TMP1.BQKHCJ='98' THEN (COALESCE(B.HNZZCYRJ,0)-COALESCE(B.RMBCKYRJ,0)-COALESCE(B.RMBLCYRJ,0))-(COALESCE(C.HNZZCYRJ,0)-COALESCE(C.RMBCKYRJ,0)-COALESCE(C.RMBLCYRJ,0)) ELSE 0 END )      -- ���°׽�ͻ������ʲ����վ����
        ,SUM(CASE WHEN TMP1.BQKHCJ='97' THEN (COALESCE(B.HNZZCYRJ,0)-COALESCE(B.RMBCKYRJ,0)-COALESCE(B.RMBLCYRJ,0))-(COALESCE(C.HNZZCYRJ,0)-COALESCE(C.RMBCKYRJ,0)-COALESCE(C.RMBLCYRJ,0)) ELSE 0 END )      -- ���»ƽ�ͻ������ʲ����վ����
        ,SUM(CASE WHEN TMP1.BQKHCJ='96' THEN (COALESCE(B.HNZZCYRJ,0)-COALESCE(B.RMBCKYRJ,0)-COALESCE(B.RMBLCYRJ,0))-(COALESCE(C.HNZZCYRJ,0)-COALESCE(C.RMBCKYRJ,0)-COALESCE(C.RMBLCYRJ,0)) ELSE 0 END )      -- ����һ����Ŀͻ������ʲ����վ����
        ,SUM(CASE WHEN TMP1.BQKHCJ='00' THEN (COALESCE(B.HNZZCYRJ,0)-COALESCE(B.RMBCKYRJ,0)-COALESCE(B.RMBLCYRJ,0))-(COALESCE(C.HNZZCYRJ,0)-COALESCE(C.RMBCKYRJ,0)-COALESCE(C.RMBLCYRJ,0)) ELSE 0 END )      -- ������ͨ�ͻ������ʲ����վ����
        ,SUM(CASE WHEN TMP1.BQXHBZ='1'  THEN (COALESCE(B.HNZZCYRJ,0)-COALESCE(B.RMBCKYRJ,0)-COALESCE(B.RMBLCYRJ,0))-(COALESCE(C.HNZZCYRJ,0)-COALESCE(C.RMBCKYRJ,0)-COALESCE(C.RMBLCYRJ,0)) ELSE 0 END )      -- ������ʧ�ͻ������ʲ����վ����
FROM    TMP1_ACRM_ACRMBB_HXKHQXFXYBB_JGBNC  TMP1
INNER   JOIN
        JCC.DSRZT_KHGSGXLSB_L              A    -- (�ͻ�������ϵ��ʷ��)
ON      TMP1.KHH=A.KHH
AND     A.LLKSRQ <= DATE '$ETL_DATE'
AND     A.LLJSRQ > DATE '$ETL_DATE'
LEFT    JOIN
        ZBC.VIEW_KHHZ_KHZCFZYRJ_M          B    -- (�ͻ��ʲ���ծ���վ�_����Ƭ
ON      TMP1.KHH=B.KHH
AND     A.KHH=B.KHH
AND     B.YWRQ=DATE '$ETL_DATE'
LEFT    JOIN
        ZBC.VIEW_KHHZ_KHZCFZYRJ_M          C    -- (�ͻ��ʲ���ծ���վ�_����Ƭ
ON      TMP1.KHH=C.KHH
AND     A.KHH=C.KHH
AND     C.YWRQ=DATE '$V_SQRQ'
WHERE TMP1.SFBQXZKH='1'--ȡ���������ͻ�
GROUP BY 1,2
;

--װ��Ŀ���������ʧ��ؿͻ�
INSERT INTO YYC.ACRM_ACRMBB_HXKHQXFXYBB_JGBNC
(
        JGH                                           -- ������
        ,NCKHKHCJDM                                   -- ����ͻ����˲㼶����
        ,YWRQ                                         -- ҵ������
        ,BYZSKHS                                      -- ������ʯ�ͻ���
        ,BYHJKHS                                      -- ���ºڽ�ͻ���
        ,BYBJKHS                                      -- ���°׽�ͻ���
        ,BYHJKHS1                                     -- ���»ƽ�ͻ���
        ,BYYBHXKHS                                    -- ����һ����Ŀͻ���
        ,BYPTKHS                                      -- ������ͨ�ͻ���
        ,BYLSKHS                                      -- ������ʧ�ͻ���
        ,KHS                                          -- �ͻ���
        ,BYZSKHXNZZCYRJYE                             -- ������ʯ�ͻ��������ʲ����վ����
        ,BYHJKHXNZZCYRJYE                             -- ���ºڽ�ͻ��������ʲ����վ����
        ,BYBJKHXNZZCYRJYE                             -- ���°׽�ͻ��������ʲ����վ����
        ,BYHJKHXNZZCYRJYE1                            -- ���»ƽ�ͻ��������ʲ����վ����
        ,BYYBHXKHXNZZCYRJYE                           -- ����һ����Ŀͻ��������ʲ����վ����
        ,BYPTKHXNZZCYRJYE                             -- ������ͨ�ͻ��������ʲ����վ����
        ,BYLSKHXNZZCYRJYE                             -- ������ʧ�ͻ��������ʲ����վ����
        ,BYZSKHCXYRJYE                                -- ������ʯ�ͻ��������վ����
        ,BYHJKHCXYRJYE                                -- ���ºڽ�ͻ��������վ����
        ,BYBJKHCXYRJYE                                -- ���°׽�ͻ��������վ����
        ,BYHJKHCXYRJYE1                               -- ���»ƽ�ͻ��������վ����
        ,BYYBHXKHCXYRJYE                              -- ����һ����Ŀͻ��������վ����
        ,BYPTKHCXYRJYE                                -- ������ͨ�ͻ��������վ����
        ,BYLSKHCXYRJYE                                -- ������ʧ�ͻ��������վ����
        ,BYZSKHLCYRJYE                                -- ������ʯ�ͻ�������վ����
        ,BYHJKHLCYRJYE                                -- ���ºڽ�ͻ�������վ����
        ,BYBJKHLCYRJYE                                -- ���°׽�ͻ�������վ����
        ,BYHJKHLCYRJYE1                               -- ���»ƽ�ͻ�������վ����
        ,BYYBHXKHLCYRJYE                              -- ����һ����Ŀͻ�������վ����
        ,BYPTKHLCYRJYE                                -- ������ͨ�ͻ�������վ����
        ,BYLSKHLCYRJYE                                -- ������ʧ�ͻ�������վ����
        ,BYZSKHQTZCYRJYE                              -- ������ʯ�ͻ������ʲ����վ����
        ,BYHJKHQTZCYRJYE                              -- ���ºڽ�ͻ������ʲ����վ����
        ,BYBJKHQTZCYRJYE                              -- ���°׽�ͻ������ʲ����վ����
        ,BYHJKHQTZCYRJYE1                             -- ���»ƽ�ͻ������ʲ����վ����
        ,BYYBHXKHQTZCYRJYE                            -- ����һ����Ŀͻ������ʲ����վ����
        ,BYPTKHQTZCYRJYE                              -- ������ͨ�ͻ������ʲ����վ����
        ,BYLSKHQTZCYRJYE                              -- ������ʧ�ͻ������ʲ����վ����
)
SELECT
        A.GSZH
        ,'12'
        ,DATE '$ETL_DATE'
        ,SUM(CASE WHEN TMP1.BQKHCJ='95' THEN 1 ELSE 0 END )                                                         -- ������ʯ�ͻ���
        ,SUM(CASE WHEN TMP1.BQKHCJ='99' THEN 1 ELSE 0 END )                                                         -- ���ºڽ�ͻ���
        ,SUM(CASE WHEN TMP1.BQKHCJ='98' THEN 1 ELSE 0 END )                                                         -- ���°׽�ͻ���
        ,SUM(CASE WHEN TMP1.BQKHCJ='97' THEN 1 ELSE 0 END )                                                         -- ���»ƽ�ͻ���
        ,SUM(CASE WHEN TMP1.BQKHCJ='96' THEN 1 ELSE 0 END )                                                         -- ����һ����Ŀͻ���
        ,SUM(CASE WHEN TMP1.BQKHCJ='00' THEN 1 ELSE 0 END )                                                         -- ������ͨ�ͻ���
        ,SUM(CASE WHEN TMP1.BQXHBZ='1'  THEN 1 ELSE 0 END )                                                         -- ������ʧ�ͻ���
        ,COUNT(1)                                                                                           -- �ͻ���

        ,SUM(CASE WHEN TMP1.BQKHCJ='95' THEN COALESCE(B.HNZZCYRJ,0)-COALESCE(C.HNZZCYRJ,0) ELSE 0 END )                                    -- ������ʯ�ͻ��������ʲ����վ����
        ,SUM(CASE WHEN TMP1.BQKHCJ='99' THEN COALESCE(B.HNZZCYRJ,0)-COALESCE(C.HNZZCYRJ,0) ELSE 0 END )                                    -- ���ºڽ�ͻ��������ʲ����վ����
        ,SUM(CASE WHEN TMP1.BQKHCJ='98' THEN COALESCE(B.HNZZCYRJ,0)-COALESCE(C.HNZZCYRJ,0) ELSE 0 END )                                    -- ���°׽�ͻ��������ʲ����վ����
        ,SUM(CASE WHEN TMP1.BQKHCJ='97' THEN COALESCE(B.HNZZCYRJ,0)-COALESCE(C.HNZZCYRJ,0) ELSE 0 END )                                    -- ���»ƽ�ͻ��������ʲ����վ����
        ,SUM(CASE WHEN TMP1.BQKHCJ='96' THEN COALESCE(B.HNZZCYRJ,0)-COALESCE(C.HNZZCYRJ,0) ELSE 0 END )                                    -- ����һ����Ŀͻ��������ʲ����վ����
        ,SUM(CASE WHEN TMP1.BQKHCJ='00' THEN COALESCE(B.HNZZCYRJ,0)-COALESCE(C.HNZZCYRJ,0) ELSE 0 END )                                    -- ������ͨ�ͻ��������ʲ����վ����
        ,SUM(CASE WHEN TMP1.BQXHBZ='1'  THEN COALESCE(B.HNZZCYRJ,0)-COALESCE(C.HNZZCYRJ,0) ELSE 0 END )                                    -- ������ʧ�ͻ��������ʲ����վ����

        ,SUM(CASE WHEN TMP1.BQKHCJ='95' THEN COALESCE(B.RMBCKYRJ,0)-COALESCE(C.RMBCKYRJ,0) ELSE 0 END )                                    -- ������ʯ�ͻ��������վ����
        ,SUM(CASE WHEN TMP1.BQKHCJ='99' THEN COALESCE(B.RMBCKYRJ,0)-COALESCE(C.RMBCKYRJ,0) ELSE 0 END )                                    -- ���ºڽ�ͻ��������վ����
        ,SUM(CASE WHEN TMP1.BQKHCJ='98' THEN COALESCE(B.RMBCKYRJ,0)-COALESCE(C.RMBCKYRJ,0) ELSE 0 END )                                    -- ���°׽�ͻ��������վ����
        ,SUM(CASE WHEN TMP1.BQKHCJ='97' THEN COALESCE(B.RMBCKYRJ,0)-COALESCE(C.RMBCKYRJ,0) ELSE 0 END )                                    -- ���»ƽ�ͻ��������վ����
        ,SUM(CASE WHEN TMP1.BQKHCJ='96' THEN COALESCE(B.RMBCKYRJ,0)-COALESCE(C.RMBCKYRJ,0) ELSE 0 END )                                    -- ����һ����Ŀͻ��������վ����
        ,SUM(CASE WHEN TMP1.BQKHCJ='00' THEN COALESCE(B.RMBCKYRJ,0)-COALESCE(C.RMBCKYRJ,0) ELSE 0 END )                                    -- ������ͨ�ͻ��������վ����
        ,SUM(CASE WHEN TMP1.BQXHBZ='1'  THEN COALESCE(B.RMBCKYRJ,0)-COALESCE(C.RMBCKYRJ,0) ELSE 0 END )                                    -- ������ʧ�ͻ��������վ����

        ,SUM(CASE WHEN TMP1.BQKHCJ='95' THEN COALESCE(B.RMBLCYRJ,0)-COALESCE(C.RMBLCYRJ,0) ELSE 0 END )                                    --������ʯ�ͻ�������վ����
        ,SUM(CASE WHEN TMP1.BQKHCJ='99' THEN COALESCE(B.RMBLCYRJ,0)-COALESCE(C.RMBLCYRJ,0) ELSE 0 END )                                    --���ºڽ�ͻ�������վ����
        ,SUM(CASE WHEN TMP1.BQKHCJ='98' THEN COALESCE(B.RMBLCYRJ,0)-COALESCE(C.RMBLCYRJ,0) ELSE 0 END )                                    --���°׽�ͻ�������վ����
        ,SUM(CASE WHEN TMP1.BQKHCJ='97' THEN COALESCE(B.RMBLCYRJ,0)-COALESCE(C.RMBLCYRJ,0) ELSE 0 END )                                    --���»ƽ�ͻ�������վ����
        ,SUM(CASE WHEN TMP1.BQKHCJ='96' THEN COALESCE(B.RMBLCYRJ,0)-COALESCE(C.RMBLCYRJ,0) ELSE 0 END )                                    --����һ����Ŀͻ�������վ����
        ,SUM(CASE WHEN TMP1.BQKHCJ='00' THEN COALESCE(B.RMBLCYRJ,0)-COALESCE(C.RMBLCYRJ,0) ELSE 0 END )                                    --������ͨ�ͻ�������վ����
        ,SUM(CASE WHEN TMP1.BQXHBZ='1'  THEN COALESCE(B.RMBLCYRJ,0)-COALESCE(C.RMBLCYRJ,0) ELSE 0 END )                                    --������ʧ�ͻ�������վ����

        ,SUM(CASE WHEN TMP1.BQKHCJ='95' THEN (COALESCE(B.HNZZCYRJ,0)-COALESCE(B.RMBCKYRJ,0)-COALESCE(B.RMBLCYRJ,0))-(COALESCE(C.HNZZCYRJ,0)-COALESCE(C.RMBCKYRJ,0)-COALESCE(C.RMBLCYRJ,0)) ELSE 0 END )      -- ������ʯ�ͻ������ʲ����վ����
        ,SUM(CASE WHEN TMP1.BQKHCJ='99' THEN (COALESCE(B.HNZZCYRJ,0)-COALESCE(B.RMBCKYRJ,0)-COALESCE(B.RMBLCYRJ,0))-(COALESCE(C.HNZZCYRJ,0)-COALESCE(C.RMBCKYRJ,0)-COALESCE(C.RMBLCYRJ,0)) ELSE 0 END )      -- ���ºڽ�ͻ������ʲ����վ����
        ,SUM(CASE WHEN TMP1.BQKHCJ='98' THEN (COALESCE(B.HNZZCYRJ,0)-COALESCE(B.RMBCKYRJ,0)-COALESCE(B.RMBLCYRJ,0))-(COALESCE(C.HNZZCYRJ,0)-COALESCE(C.RMBCKYRJ,0)-COALESCE(C.RMBLCYRJ,0)) ELSE 0 END )      -- ���°׽�ͻ������ʲ����վ����
        ,SUM(CASE WHEN TMP1.BQKHCJ='97' THEN (COALESCE(B.HNZZCYRJ,0)-COALESCE(B.RMBCKYRJ,0)-COALESCE(B.RMBLCYRJ,0))-(COALESCE(C.HNZZCYRJ,0)-COALESCE(C.RMBCKYRJ,0)-COALESCE(C.RMBLCYRJ,0)) ELSE 0 END )      -- ���»ƽ�ͻ������ʲ����վ����
        ,SUM(CASE WHEN TMP1.BQKHCJ='96' THEN (COALESCE(B.HNZZCYRJ,0)-COALESCE(B.RMBCKYRJ,0)-COALESCE(B.RMBLCYRJ,0))-(COALESCE(C.HNZZCYRJ,0)-COALESCE(C.RMBCKYRJ,0)-COALESCE(C.RMBLCYRJ,0)) ELSE 0 END )      -- ����һ����Ŀͻ������ʲ����վ����
        ,SUM(CASE WHEN TMP1.BQKHCJ='00' THEN (COALESCE(B.HNZZCYRJ,0)-COALESCE(B.RMBCKYRJ,0)-COALESCE(B.RMBLCYRJ,0))-(COALESCE(C.HNZZCYRJ,0)-COALESCE(C.RMBCKYRJ,0)-COALESCE(C.RMBLCYRJ,0)) ELSE 0 END )      -- ������ͨ�ͻ������ʲ����վ����
        ,SUM(CASE WHEN TMP1.BQXHBZ='1'  THEN (COALESCE(B.HNZZCYRJ,0)-COALESCE(B.RMBCKYRJ,0)-COALESCE(B.RMBLCYRJ,0))-(COALESCE(C.HNZZCYRJ,0)-COALESCE(C.RMBCKYRJ,0)-COALESCE(C.RMBLCYRJ,0)) ELSE 0 END )      -- ������ʧ�ͻ������ʲ����վ����
FROM    TMP1_ACRM_ACRMBB_HXKHQXFXYBB_JGBNC  TMP1
INNER   JOIN
        JCC.DSRZT_KHGSGXLSB_L              A    -- (�ͻ�������ϵ��ʷ��)
ON      TMP1.KHH=A.KHH
AND     A.LLKSRQ <= DATE '$ETL_DATE'
AND     A.LLJSRQ > DATE '$ETL_DATE'
LEFT    JOIN
        ZBC.VIEW_KHHZ_KHZCFZYRJ_M          B    -- (�ͻ��ʲ���ծ���վ�_����Ƭ
ON      TMP1.KHH=B.KHH
AND     A.KHH=B.KHH
AND     B.YWRQ=DATE '$ETL_DATE'
LEFT    JOIN
        ZBC.VIEW_KHHZ_KHZCFZYRJ_M          C    -- (�ͻ��ʲ���ծ���վ�_����Ƭ
ON      TMP1.KHH=C.KHH
AND     A.KHH=C.KHH
AND     C.YWRQ=DATE '$V_SQRQ'
WHERE TMP1.SFBQLSWHKH='1' --ȡ��ʧ��ؿͻ�
GROUP BY 1,2
;
COMMIT;