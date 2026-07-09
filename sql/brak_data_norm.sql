-- Normalized typed view over brak_team.brak_data (all-text source).
-- Main report amount = "Сумма факт, руб" (legacy write_offs.amount).
-- New columns are appended at the end so CREATE OR REPLACE VIEW stays compatible.

CREATE OR REPLACE VIEW brak_team.brak_data_norm AS
SELECT
    NULLIF(BTRIM(shk_id), '')::bigint AS shk_id,
    NULLIF(BTRIM(date), '')::timestamp AS date,
    type,
    NULLIF(REPLACE(BTRIM("Фактическая ст-ть, руб"), ',', '.'), '')::numeric AS total_cost,
    NULLIF(REPLACE(BTRIM("Сумма факт, руб"), ',', '.'), '')::numeric AS amount,
    NULLIF(REPLACE(BTRIM("Доля"), ',', '.'), '')::numeric AS share,
    NULLIF(BTRIM(office_id), '')::integer AS office_id,
    NULLIF(BTRIM(wh_id), '')::integer AS wh_id,
    NULLIF(BTRIM(nm_id), '')::bigint AS nm_id,
    subject_name,
    parent_name,
    title,
    brand_name,
    state_id,
    NULLIF(BTRIM(reason_id), '')::integer AS reason_id,
    reason_descr,
    NULLIF(BTRIM(seller_id), '')::bigint AS seller_id,
    NULLIF(BTRIM(supplier_id), '')::bigint AS supplier_id,
    owner_product,
    NULLIF(REPLACE(BTRIM("ORG_do_braka"), ',', '.'), '')::integer AS cnt_org,
    NULLIF(REPLACE(BTRIM("Кол-во ORS до брака"), ',', '.'), '')::integer AS cnt_ors,
    NULLIF(REPLACE(BTRIM("Кол-во OCR до брака"), ',', '.'), '')::integer AS cnt_ocr,
    NULLIF(REPLACE(BTRIM("Общая ст-ть, руб"), ',', '.'), '')::numeric AS amount_obsh,
    NULLIF(REPLACE(BTRIM(summa_obshay), ',', '.'), '')::numeric AS summa_obshay,
    NULLIF(BTRIM(wh_name), '') AS wh_name,
    NULLIF(BTRIM(source_file), '') AS source_file
FROM brak_team.brak_data;
