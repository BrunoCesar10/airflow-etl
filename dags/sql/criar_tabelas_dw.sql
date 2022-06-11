CREATE TABLE IF NOT EXISTS DM_Disciplina (
        id_disc text  NOT NULL,
        nome_disc text NOT NULL,
        carga_horaria text NOT NULL,
        PRIMARY KEY (id_disc)
);
CREATE TABLE IF NOT EXISTS DM_Tempo (
        id_tempo text NOT NULL,
        semestre text NOT NULL,
        PRIMARY KEY (id_tempo)
);
CREATE TABLE IF NOT EXISTS DM_Curso (
        id_curso text NOT NULL,
        nom_curso text NOT NULL,
        PRIMARY KEY (id_curso)
);
CREATE TABLE IF NOT EXISTS FT_Reprovacao_GC (
        id_tempo text NOT NULL,
        id_disc text NOT NULL,
        total_rep_cot_disc text NOT NULL,
        total_mat_alu_cot text NOT NULL,
        total_reprovados text NOT NULL,
        total_mat text NOT NULL
);
CREATE TABLE IF NOT EXISTS FT_Reprovacao (
        id_tempo text NOT NULL,
        id_disc text NOT NULL,
        id_curso text NOT NULL,
        id_dpto text NOT NULL,
        total_rep_disc text NOT NULL,
        total_mat_alu text NOT NULL
);
CREATE TABLE IF NOT EXISTS DM_Departamento (
        id_dpto text NOT NULL,
        nom_dpto text NOT NULL,
        PRIMARY KEY (id_dpto)
);
ALTER TABLE FT_Reprovacao_GC ADD FOREIGN KEY (id_tempo) REFERENCES DM_Tempo(id_tempo);
ALTER TABLE FT_Reprovacao_GC ADD FOREIGN KEY (id_disc) REFERENCES DM_Disciplina(id_disc);
ALTER TABLE FT_Reprovacao ADD FOREIGN KEY (id_curso) REFERENCES DM_Curso(id_curso);
ALTER TABLE FT_Reprovacao ADD FOREIGN KEY (id_tempo) REFERENCES DM_Tempo(id_tempo);
ALTER TABLE FT_Reprovacao ADD FOREIGN KEY (id_dpto) REFERENCES DM_Departamento(id_dpto);
ALTER TABLE FT_Reprovacao ADD FOREIGN KEY (id_disc) REFERENCES DM_Disciplina(id_disc);