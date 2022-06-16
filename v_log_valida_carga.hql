CREATE VIEW analytical.v_log_valida_carga AS
SELECT
p.cd_sequencial,
p.nm_processo,
p.nm_objeto,
p.qt_registros_previstos,
i.qt_registros_inseridos,
CASE WHEN p.qt_registros_previstos != i.qt_registros_inseridos THEN 'Success' ELSE 'Failure' END status,
p.dt_execucao,
p.aa_carga
FROM analytical.a_estrutural_log_validacao_carga_prevista p
LEFT JOIN analytical.a_estrutural_log_validacao_carga_inserida i
ON p.cd_sequencial = i.cd_sequencial
AND p.nm_processo = i.nm_processo
AND p.nm_objeto = i.nm_objeto
AND p.dt_execucao = i.dt_execucao;