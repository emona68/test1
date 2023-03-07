create or replace PACKAGE BODY          "CREATE_SCRIPTS"
-- ????
--- ???
---- ????
IS
  CURSOR c_scripts(p_script_name IN VARCHAR2)
      IS SELECT id
               ,name
           FROM scripts
          WHERE name = p_script_name
          ORDER BY 1
  ;

  CURSOR c_text(p_script_id IN NUMBER)
      IS SELECT blc.id block_id
               ,text
           FROM scripts                scr
               ,blocks                 blc
               ,block_lines            blc_line
               ,block_groups           blc_grp
               ,block_group_blocks     blc_grp_blc
          WHERE scr.id                     = p_script_id
            AND scr.block_group_id         = blc_grp.id
            AND blc_grp_blc.block_group_id = blc_grp.id
            AND blc_grp_blc.block_id       = blc.id
            AND blc_line.block_id          = blc.id
          ORDER BY blc_grp_blc.order_no
                  ,blc_line.line
  ;

  CURSOR c_args(p_script_id IN NUMBER, p_block_id IN NUMBER)
      IS SELECT blc_arg.argument_name
               ,scr_blc_arg.value
           FROM scripts                scr
               ,block_arguments        blc_arg
               ,script_block_arguments scr_blc_arg
          WHERE scr.id                          = p_script_id
            AND scr_blc_arg.block_argument_id   = blc_arg.id
            AND scr_blc_arg.script_id           = scr.id
            AND blc_arg.block_id               <= p_block_id
          ORDER BY scr_blc_arg.block_argument_id DESC
  ;

  TYPE t_filelist IS TABLE OF VARCHAR2(100) INDEX BY BINARY_INTEGER;
  g_filelist t_filelist;

  TYPE t_rec_arg IS RECORD
  ( block_id  NUMBER
   ,arg_name  VARCHAR2(100)
   ,arg_value VARCHAR2(100)
  );
  TYPE t_tab_arg IS TABLE OF t_rec_arg INDEX BY BINARY_INTEGER;
  g_tab_arg t_tab_arg;

  TYPE t_rec_blocks IS RECORD
  ( block_id  NUMBER
   ,order_no  NUMBER
  );

  TYPE t_tab_blocks IS TABLE OF t_rec_blocks INDEX BY BINARY_INTEGER;
  g_tab_blocks t_tab_blocks;

  g_file_name VARCHAR2(100);

  FUNCTION open_file( p_directory_name IN VARCHAR2, p_file_name IN VARCHAR2 ) return utl_file.file_type
  IS
  BEGIN
    return utl_file.fopen(location => p_directory_name, filename => p_file_name, open_mode => 'r', max_linesize => 32760);
  EXCEPTION
    WHEN others THEN
      dbms_output.put_line('Exception in open_file (p_file_name=' || p_file_name || '): ' || SQLERRM);
      raise;
  END open_file;

  PROCEDURE close_file( p_file IN OUT utl_file.file_type )
  IS
  BEGIN
    utl_file.fclose(p_file);
  EXCEPTION
    WHEN others THEN
      dbms_output.put_line('Exception in close_file: ' || SQLERRM);
      raise;
  END close_file;

  PROCEDURE read_filelist(p_file IN OUT utl_file.file_type)
  IS
    l_file_name VARCHAR2(1000);
  BEGIN
    g_filelist.DELETE;
    LOOP
      utl_file.get_line(file => p_file, buffer => l_file_name);
      g_filelist(nvl(g_filelist.COUNT,0)+1) := l_file_name;
    END LOOP;
  EXCEPTION
    WHEN no_data_found THEN
      close_file(p_file);
    WHEN others THEN
      dbms_output.put_line('Exception in read_filelist: ' || SQLERRM);
      raise;
  END read_filelist;

  FUNCTION get_script_type_from_blocks RETURN NUMBER
  IS
    --i                    BINARY_INTEGER;
    l_ok                 BOOLEAN        := FALSE;
    l_block_group_id     NUMBER;
    l_line               NUMBER;
    l_cnt                NUMBER;
    l_cnt_actual_group   NUMBER;
    l_cnt_existing_group NUMBER;
  BEGIN
    l_line := 10;
    FORALL i IN g_tab_blocks.FIRST..g_tab_blocks.LAST
      INSERT INTO tmp_block_groups(block_id,order_no)
      VALUES(g_tab_blocks(i).block_id,g_tab_blocks(i).order_no)
    ;

    SELECT count(*)
      INTO l_cnt_actual_group
      FROM tmp_block_groups
    ;

    FOR r_block_groups IN(SELECT id,count(*) cnt FROM block_groups GROUP BY id ORDER BY id) LOOP
      l_line := 20;
      EXIT WHEN l_ok;
      l_block_group_id := r_block_groups.id;
      SELECT count(*)
        INTO l_cnt_existing_group
        FROM block_group_blocks
       WHERE block_group_id = r_block_groups.id
      ;
      SELECT count(*)
        INTO l_cnt
        FROM( SELECT block_id,order_no
                FROM block_group_blocks
               WHERE block_group_id = r_block_groups.id
              INTERSECT
              SELECT block_id,order_no
                FROM tmp_block_groups
             );
      IF(g_trace) THEN
        dbms_output.put_line('block id: ' || l_block_group_id || ', l_cnt_actual_group:   ' || l_cnt_actual_group);
        dbms_output.put_line('block id: ' || l_block_group_id || ', l_cnt_existing_group: ' || l_cnt_existing_group);
        dbms_output.put_line('block id: ' || l_block_group_id || ', l_cnt:     ' || l_cnt);
      END IF;
      IF(l_cnt = l_cnt_actual_group) AND (l_cnt = l_cnt_existing_group) THEN
        l_ok := TRUE;
        EXIT;
      END IF;
    END LOOP;
    COMMIT;
    --
    IF(NOT l_ok) THEN
      return 0;
    ELSE
      return l_block_group_id;
    END IF;
  EXCEPTION
    WHEN others THEN
      dbms_output.put_line('Exception in get_script_type_from_blocks (' || to_char(l_line) || '): ' || SQLERRM);
      raise;
  END get_script_type_from_blocks;

  FUNCTION get_script_type(p_file_name IN VARCHAR2) RETURN NUMBER
  IS
    l_file      utl_file.file_type;
    i_file      BINARY_INTEGER;
    l_line      NUMBER;
    l_type_1    BOOLEAN := TRUE;
    l_type_2    BOOLEAN := TRUE;
    l_type_3    BOOLEAN := TRUE;
    l_type_4    BOOLEAN := TRUE;
    l_delimiter CHAR(1);
    l_script_type                  NUMBER;

    l_text                         VARCHAR2(1000);
    l_block_active                 BOOLEAN := FALSE;
    l_block_id                     NUMBER;

    l_block_doc                    BOOLEAN := FALSE;
    l_block_doc_id                 NUMBER  := 1;
    l_block_doc_nochange           BOOLEAN := FALSE;
    l_block_doc_nochange_id        NUMBER  := 2;
    l_block_doc_end                BOOLEAN := FALSE;
    l_block_doc_end_id             NUMBER  := 3;
    l_block_doc_change             BOOLEAN := FALSE;
    l_block_doc_change1            BOOLEAN := FALSE;
    l_block_doc_change1_id         NUMBER  := 19;
    l_block_doc_change2            BOOLEAN := FALSE;
    l_block_doc_change2_id         NUMBER  := 26;
    l_block_env                    BOOLEAN := FALSE;
    l_block_env_id                 NUMBER  := 4;
    l_block_log                    BOOLEAN := FALSE;
    l_block_log_id                 NUMBER  := 5;
    l_block_start                  BOOLEAN := FALSE;
    l_block_start_id               NUMBER  := 6;
    l_block_chk_infile             BOOLEAN := FALSE;
    l_block_chk_infile_id          NUMBER  := 7;
    l_block_sed                    BOOLEAN := FALSE;
    l_block_sed_id                 NUMBER  := 8;
    l_block_echo_truncate          BOOLEAN := FALSE;
    l_block_echo_truncate_id       NUMBER  := 9;
    l_block_truncate               BOOLEAN := FALSE;
    l_block_truncate_id            NUMBER  := 10;
    l_block_dbload                 BOOLEAN := FALSE;
    l_block_dbload_id              NUMBER  := 11;
    l_block_chk_dbload             BOOLEAN := FALSE;
    l_block_chk_dbload_id          NUMBER  := 12;
    l_block_chk_isbad              BOOLEAN := FALSE;
    l_block_chk_isbad_id           NUMBER  := 13;
    l_block_rm_loadfile            BOOLEAN := FALSE;
    l_block_rm_loadfile_id         NUMBER  := 14;
    l_block_compress_logfile       BOOLEAN := FALSE;
    l_block_compress_logfile_id    NUMBER  := 15;
    l_block_gzip_active            BOOLEAN := FALSE;
    l_block_statistics             BOOLEAN := FALSE;
    l_block_statistics_id          NUMBER  := 16;
    l_block_chk_return             BOOLEAN := FALSE;
    l_block_chk_return_id          NUMBER  := 17;
    l_block_return                 BOOLEAN := FALSE;
    l_block_return_id              NUMBER  := 18;
    l_block_ln                     BOOLEAN := FALSE;
    l_block_ln_id                  NUMBER  := 20;
    l_block_dbload_para            BOOLEAN := FALSE;
    l_block_dbload_para_id         NUMBER  := 21;
    l_block_sqlldr                 BOOLEAN := FALSE;
    l_block_sqlldr_id              NUMBER  := 22;
    l_block_nachbehandlung         BOOLEAN := FALSE;
    l_block_nachbehandlung_id      NUMBER  := 23;
    l_block_chk_nachbehandlung     BOOLEAN := FALSE;
    l_block_chk_nachbehandlung_id  NUMBER  := 24;
    l_block_statistics_short       BOOLEAN := FALSE;
    l_block_statistics_short_id    NUMBER  := 25;

    l_table_name_stored            BOOLEAN := FALSE;
    l_loadfile_stored              BOOLEAN := FALSE;
    l_comfile_stored               BOOLEAN := FALSE;

    l_new_block_group_id           NUMBER;
    l_new_block_group_block_id     NUMBER;
    l_new_block_tab_id             NUMBER;
    l_cnt                          NUMBER;
  BEGIN
    l_line := 10;
    g_tab_blocks.DELETE;
    g_tab_arg.DELETE;
    --
    IF(p_file_name = 'atf_bewertung_bas.sh') THEN
      return -1;
    END IF;
    l_file := utl_file.fopen(location => g_directory_name, filename => p_file_name, open_mode => 'r', max_linesize => 32760);
    l_line := 20;
    BEGIN
      LOOP
        l_line := 30;
        utl_file.get_line(file => l_file, buffer => l_text);
        l_line := 31;
        -- empty lines
        IF(trim(l_text) IS NULL) OR (trim(l_text) = '#') THEN
          l_line := 40;
          NULL;
        -- 1 Doc
        ELSIF(NOT l_block_active) AND ((l_text LIKE '# Projekt:%') OR (l_text LIKE '#! /bin/ksh%')) THEN
          l_line := 50;
          -- start doc block
          l_block_doc              := TRUE;
          l_block_active           := TRUE;
          l_block_id               := l_block_doc_id;
          --
          l_new_block_tab_id := nvl(g_tab_blocks.COUNT,0)+1;
          g_tab_blocks(l_new_block_tab_id).block_id := l_block_id;
          g_tab_blocks(l_new_block_tab_id).order_no := l_new_block_tab_id;
          --
          g_tab_arg(nvl(g_tab_arg.LAST,0)+1).arg_name := 'PROJECTNAME';
          g_tab_arg(g_tab_arg.LAST).arg_value  := substr(l_text,12,6);
          g_tab_arg(g_tab_arg.LAST).block_id   := l_block_id;
        ELSIF(l_block_doc) AND (l_text LIKE '# Prozedurname:%') THEN
          l_line := 60;
          g_tab_arg(nvl(g_tab_arg.LAST,0)+1).arg_name := 'SCRIPTNAME';
          g_tab_arg(g_tab_arg.LAST).arg_value  := substr(l_text,17);
          g_tab_arg(g_tab_arg.LAST).block_id   := l_block_id;
          --
          g_tab_arg(nvl(g_tab_arg.LAST,0)+1).arg_name := 'BASENAME';
          g_tab_arg(g_tab_arg.LAST).arg_value  := substr(g_tab_arg(g_tab_arg.LAST-1).arg_value,1,instr(g_tab_arg(g_tab_arg.LAST-1).arg_value,'.sh',-1,1)-1);
          g_tab_arg(g_tab_arg.LAST).block_id   := l_block_id;
        ELSIF(l_block_doc) AND (l_text LIKE '# Autor:%') THEN
          l_line := 70;
          g_tab_arg(nvl(g_tab_arg.LAST,0)+1).arg_name := 'AUTHOR';
          g_tab_arg(g_tab_arg.LAST).arg_value  := substr(l_text,10);
          g_tab_arg(g_tab_arg.LAST).block_id   := l_block_id;
        ELSIF(l_block_doc) AND (l_text LIKE '# Erstellungs%') THEN
          l_line := 80;
          g_tab_arg(nvl(g_tab_arg.LAST,0)+1).arg_name := 'SCRIPTDATE';
          g_tab_arg(g_tab_arg.LAST).arg_value  := substr(l_text,32);
          g_tab_arg(g_tab_arg.LAST).block_id   := l_block_id;
        /*
        ELSIF(l_block_doc) AND (l_text LIKE '# Aenderungshistorie%') THEN
          l_line := 90;
          l_block_doc_change       := TRUE;
        ELSIF(l_block_doc_change) AND (l_text NOT LIKE '# ---%') AND (NOT l_block_doc_change1) THEN
          l_line := 100;
          l_block_doc_change1      := TRUE;
          l_block_id               := l_block_doc_change2_id;
          --
          l_new_block_tab_id := nvl(g_tab_blocks.COUNT,0)+1;
          g_tab_blocks(l_new_block_tab_id).block_id := l_block_id;
          g_tab_blocks(l_new_block_tab_id).order_no := l_new_block_tab_id;
          --
          l_block_id               := l_block_doc_change1_id;
          --
          l_new_block_tab_id := nvl(g_tab_blocks.COUNT,0)+1;
          g_tab_blocks(l_new_block_tab_id).block_id := l_block_id;
          g_tab_blocks(l_new_block_tab_id).order_no := l_new_block_tab_id;
          --
          l_text := ltrim(ltrim(l_text,'#'),' ');
          IF(instr(l_text,' ',1,1) < instr(l_text,',',1,1)) THEN
            l_delimiter := ' ';
          ELSE
            l_delimiter := ',';
          END IF;
          l_line := 110;
          g_tab_arg(nvl(g_tab_arg.LAST,0)+1).arg_name := 'HISTORYUSER1';
          g_tab_arg(g_tab_arg.LAST).block_id          := l_block_id;
          g_tab_arg(g_tab_arg.LAST).arg_value         := substr(l_text,1,instr(l_text,l_delimiter,1,1)-1);
          l_text := ltrim(substr(l_text,instr(l_text,l_delimiter,1,1)+1));
          IF(l_text LIKE '20%') THEN
            l_line := 120;
            g_tab_arg(nvl(g_tab_arg.LAST,0)+1).arg_name := 'HISTORYDATE1';
            g_tab_arg(g_tab_arg.LAST).block_id          := l_block_id;
            g_tab_arg(g_tab_arg.LAST).arg_value := substr(l_text,1,instr(l_text,l_delimiter,1,1)-1);
            l_text := ltrim(substr(l_text,instr(l_text,l_delimiter,1,1)+1));
            g_tab_arg(nvl(g_tab_arg.LAST,0)+1).arg_name := 'HISTORYTEXT1';
            g_tab_arg(g_tab_arg.LAST).block_id          := l_block_id;
            g_tab_arg(g_tab_arg.LAST).arg_value         := substr(l_text,1);
          ELSE
            l_line := 130;
            g_tab_arg(nvl(g_tab_arg.LAST,0)+1).arg_name := 'HISTORYTEXT1';
            g_tab_arg(g_tab_arg.LAST).block_id          := l_block_id;
            g_tab_arg(g_tab_arg.LAST).arg_value         := substr(l_text,1,instr(l_text,l_delimiter,1,1)-1);
            l_text := ltrim(substr(l_text,instr(l_text,l_delimiter,1,1)+1));
            g_tab_arg(nvl(g_tab_arg.LAST,0)+1).arg_name := 'HISTORYDATE1';
            g_tab_arg(g_tab_arg.LAST).block_id          := l_block_id;
            g_tab_arg(g_tab_arg.LAST).arg_value := substr(l_text,1);
          END IF;
        ELSIF(l_block_doc_change1) AND (l_text NOT LIKE '# ---%') THEN
          l_line := 140;
          l_block_doc_change2      := TRUE;
          l_block_id               := l_block_doc_change2_id;
          --
          l_text := ltrim(ltrim(l_text,'#'),' ');
          IF(instr(l_text,' ',1,1) < instr(l_text,',',1,1)) THEN
            l_delimiter := ' ';
          ELSE
            l_delimiter := ',';
          END IF;
          l_line := 150;
          g_tab_arg(nvl(g_tab_arg.LAST,0)+1).arg_name := 'HISTORYUSER2';
          g_tab_arg(g_tab_arg.LAST).block_id          := l_block_id;
          g_tab_arg(g_tab_arg.LAST).arg_value         := substr(l_text,1,instr(l_text,l_delimiter,1,1)-1);
          l_text := ltrim(substr(l_text,instr(l_text,l_delimiter,1,1)+1));
          IF(l_text LIKE '20%') THEN
            l_line := 160;
            g_tab_arg(nvl(g_tab_arg.LAST,0)+1).arg_name := 'HISTORYDATE2';
            g_tab_arg(g_tab_arg.LAST).block_id          := l_block_id;
            g_tab_arg(g_tab_arg.LAST).arg_value := substr(l_text,1,instr(l_text,l_delimiter,1,1)-1);
            l_text := ltrim(substr(l_text,instr(l_text,l_delimiter,1,1)+1));
            g_tab_arg(nvl(g_tab_arg.LAST,0)+1).arg_name := 'HISTORYTEXT2';
            g_tab_arg(g_tab_arg.LAST).block_id          := l_block_id;
            g_tab_arg(g_tab_arg.LAST).arg_value         := substr(l_text,1);
          ELSE
            l_line := 170;
            g_tab_arg(nvl(g_tab_arg.LAST,0)+1).arg_name := 'HISTORYTEXT2';
            g_tab_arg(g_tab_arg.LAST).block_id          := l_block_id;
            g_tab_arg(g_tab_arg.LAST).arg_value         := substr(l_text,1,instr(l_text,l_delimiter,1,1)-1);
            l_text := ltrim(substr(l_text,instr(l_text,l_delimiter,1,1)+1));
            g_tab_arg(nvl(g_tab_arg.LAST,0)+1).arg_name := 'HISTORYDATE2';
            g_tab_arg(g_tab_arg.LAST).block_id          := l_block_id;
            g_tab_arg(g_tab_arg.LAST).arg_value := substr(l_text,1);
          END IF;
        */
        ELSIF(l_block_doc) AND ((l_text LIKE '# ---%') OR (l_text LIKE '###%') OR (l_text LIKE 'NAME=`basename $0`%')) THEN
          l_line := 180;
          /*
          IF(NOT l_block_doc_change1) THEN
            l_block_id               := l_block_doc_nochange_id;
            --
            l_new_block_tab_id := nvl(g_tab_blocks.COUNT,0)+1;
            g_tab_blocks(l_new_block_tab_id).block_id := l_block_id;
            g_tab_blocks(l_new_block_tab_id).order_no := l_new_block_tab_id;
            --
          END IF;
          */
          l_line := 190;
          l_block_id               := l_block_doc_end_id;
          --
          l_new_block_tab_id := nvl(g_tab_blocks.COUNT,0)+1;
          g_tab_blocks(l_new_block_tab_id).block_id := l_block_id;
          g_tab_blocks(l_new_block_tab_id).order_no := l_new_block_tab_id;
          --
          l_block_doc              := FALSE;
          l_block_doc_change       := FALSE;
          l_block_doc_change1      := FALSE;
          l_block_doc_change2      := FALSE;
          l_block_active           := FALSE;
        -- 4 environment
        ELSIF(NOT l_block_active) AND (l_text LIKE 'NAME=`basename $0`%') THEN
          l_line := 200;
          l_block_env              := TRUE;
          l_block_active           := TRUE;
          l_block_id               := l_block_env_id;
          --
          l_new_block_tab_id := nvl(g_tab_blocks.COUNT,0)+1;
          g_tab_blocks(l_new_block_tab_id).block_id := l_block_id;
          g_tab_blocks(l_new_block_tab_id).order_no := l_new_block_tab_id;
          --
        ELSIF(l_block_active) AND (l_block_env) AND (l_text LIKE '. $CONFIGPATH/p_env.sh%') THEN
          l_line := 200;
          l_block_env              := FALSE;
          l_block_active           := FALSE;
        -- 5 logfile
        ELSIF(NOT l_block_active) AND (l_text LIKE '## logfile = %') THEN
          l_line := 210;
          l_block_log              := TRUE;
          l_block_active           := TRUE;
          l_block_id               := l_block_log_id;
          --
          l_new_block_tab_id := nvl(g_tab_blocks.COUNT,0)+1;
          g_tab_blocks(l_new_block_tab_id).block_id := l_block_id;
          g_tab_blocks(l_new_block_tab_id).order_no := l_new_block_tab_id;
          --
        ELSIF(l_block_active) AND (l_block_log) AND (l_text LIKE 'LOGFILE=`echo %') THEN
          l_line := 220;
          l_block_log              := FALSE;
          l_block_active           := FALSE;
        -- 6 p_start.sh
        ELSIF(NOT l_block_active) AND (l_text LIKE '. p_start.sh%') THEN
          l_line := 230;
          l_block_id               := l_block_start_id;
          --
          l_new_block_tab_id := nvl(g_tab_blocks.COUNT,0)+1;
          g_tab_blocks(l_new_block_tab_id).block_id := l_block_id;
          g_tab_blocks(l_new_block_tab_id).order_no := l_new_block_tab_id;
          --
        -- 7 check infile
        ELSIF(NOT l_block_active) AND (l_text LIKE 'if [ ! -f $DATAPATH/incoming/$name ]%') THEN
          l_line := 240;
          l_block_chk_infile       := TRUE;
          l_block_active           := TRUE;
          l_block_id               := l_block_chk_infile_id;
          --
          l_new_block_tab_id := nvl(g_tab_blocks.COUNT,0)+1;
          g_tab_blocks(l_new_block_tab_id).block_id := l_block_id;
          g_tab_blocks(l_new_block_tab_id).order_no := l_new_block_tab_id;
          --
        ELSIF(l_block_active) AND (l_block_chk_infile) AND (l_text LIKE 'fi%') THEN
          l_line := 250;
          l_block_chk_infile       := FALSE;
          l_block_active           := FALSE;
        -- 8 sed
        ELSIF(NOT l_block_active) AND (l_text LIKE 'sed %') THEN
          l_line := 260;
          l_block_id               := l_block_sed_id;
          --
          l_new_block_tab_id := nvl(g_tab_blocks.COUNT,0)+1;
          g_tab_blocks(l_new_block_tab_id).block_id := l_block_id;
          g_tab_blocks(l_new_block_tab_id).order_no := l_new_block_tab_id;
          --
          g_tab_arg(nvl(g_tab_arg.LAST,0)+1).arg_name := 'LOADFILE';
          g_tab_arg(g_tab_arg.LAST).block_id          := l_block_id;
          g_tab_arg(g_tab_arg.LAST).arg_value         := substr(l_text,instr(l_text,'/',-1,1)+1);
          l_loadfile_stored                           := TRUE;
        -- 9 truncate info (output)
        ELSIF(NOT l_block_active) AND (l_text LIKE 'echo "TRUNCATE TABLE %') AND (l_text NOT LIKE '%| dbaccess%') THEN
          l_line := 270;
          l_block_id               := l_block_echo_truncate_id;
          --
          l_new_block_tab_id := nvl(g_tab_blocks.COUNT,0)+1;
          g_tab_blocks(l_new_block_tab_id).block_id := l_block_id;
          g_tab_blocks(l_new_block_tab_id).order_no := l_new_block_tab_id;
          --
          g_tab_arg(nvl(g_tab_arg.LAST,0)+1).arg_name := 'TABLENAME';
          g_tab_arg(g_tab_arg.LAST).block_id          := l_block_id;
          g_tab_arg(g_tab_arg.LAST).arg_value         := substr(l_text,length('echo "TRUNCATE TABLE ')+1,instr(l_text,'...',-1,1)-length('echo "TRUNCATE TABLE ')-1);
          l_table_name_stored                         := TRUE;
        -- 10 truncate execute
        ELSIF(NOT l_block_active) AND (l_text LIKE 'echo "TRUNCATE TABLE %| dbaccess%') THEN
          l_line := 280;
          l_block_id               := l_block_truncate_id;
          --
          l_new_block_tab_id := nvl(g_tab_blocks.COUNT,0)+1;
          g_tab_blocks(l_new_block_tab_id).block_id := l_block_id;
          g_tab_blocks(l_new_block_tab_id).order_no := l_new_block_tab_id;
          --
        -- 11 dbload
        ELSIF(NOT l_block_active) AND (l_text LIKE 'dbload -d $DBNAME -k -c%') AND (l_text NOT LIKE 'dbload -d $DBNAME%$SQLLDR_MAXERR%') THEN
          l_line := 290;
          l_block_dbload           := TRUE;
          l_block_active           := TRUE;
          l_block_id               := l_block_dbload_id;
          --
          l_new_block_tab_id := nvl(g_tab_blocks.COUNT,0)+1;
          g_tab_blocks(l_new_block_tab_id).block_id := l_block_id;
          g_tab_blocks(l_new_block_tab_id).order_no := l_new_block_tab_id;
          --
          g_tab_arg(nvl(g_tab_arg.LAST,0)+1).arg_name := 'COMFILE';
          g_tab_arg(g_tab_arg.LAST).block_id          := l_block_id;
          g_tab_arg(g_tab_arg.LAST).arg_value         := substr(l_text,instr(l_text,'/',-1,1)+1,instr(l_text,' ',instr(l_text,'/',-1,1)+1,1)-instr(l_text,'/',-1,1));
          l_comfile_stored                            := TRUE;
        -- 12 check dbload
        ELSIF(l_block_active) AND (l_block_dbload) AND (l_text LIKE 'rc=$%') THEN
          l_line := 300;
          l_block_dbload           := FALSE;
          l_block_chk_dbload       := TRUE;
          l_block_id               := l_block_chk_dbload_id;
          --
          l_new_block_tab_id := nvl(g_tab_blocks.COUNT,0)+1;
          g_tab_blocks(l_new_block_tab_id).block_id := l_block_id;
          g_tab_blocks(l_new_block_tab_id).order_no := l_new_block_tab_id;
          --
        ELSIF(l_block_active) AND (l_block_chk_dbload) AND (l_text LIKE 'fi%') THEN
          l_line := 310;
          l_block_chk_dbload       := FALSE;
          l_block_active           := FALSE;
        -- 13 grep "is bad" in logfile
        ELSIF(NOT l_block_active) AND (l_text LIKE 'grep%') THEN
          l_line := 320;
          l_block_chk_isbad        := TRUE;
          l_block_active           := TRUE;
          l_block_id               := l_block_chk_isbad_id;
          --
          l_new_block_tab_id := nvl(g_tab_blocks.COUNT,0)+1;
          g_tab_blocks(l_new_block_tab_id).block_id := l_block_id;
          g_tab_blocks(l_new_block_tab_id).order_no := l_new_block_tab_id;
          --
        ELSIF(l_block_active) AND (l_block_chk_isbad) AND (l_text LIKE 'fi%') THEN
          l_line := 330;
          l_block_chk_isbad        := FALSE;
          l_block_active           := FALSE;
        -- 14 remove loadfile
        ELSIF(NOT l_block_active) AND (l_text LIKE 'rm $DATAPATH/incoming/%') THEN
          l_line := 340;
          l_block_id               := l_block_rm_loadfile_id;
          --
          l_new_block_tab_id := nvl(g_tab_blocks.COUNT,0)+1;
          g_tab_blocks(l_new_block_tab_id).block_id := l_block_id;
          g_tab_blocks(l_new_block_tab_id).order_no := l_new_block_tab_id;
          --
        -- 15 compress logfile
        ELSIF(NOT l_block_active) AND (l_text LIKE 'if [ ! -s $LOGFILE ]%') THEN
          l_line := 350;
          l_block_compress_logfile := TRUE;
          l_block_active           := TRUE;
          l_block_id               := l_block_compress_logfile_id;
          --
          l_new_block_tab_id := nvl(g_tab_blocks.COUNT,0)+1;
          g_tab_blocks(l_new_block_tab_id).block_id := l_block_id;
          g_tab_blocks(l_new_block_tab_id).order_no := l_new_block_tab_id;
          --
        ELSIF(l_block_compress_logfile) AND (l_text LIKE '%gzip%') THEN
          l_line := 360;
          l_block_gzip_active      := TRUE;
        ELSIF(l_block_gzip_active) AND (l_text LIKE '%mv $DATAPATH/incoming/$name.gz%') THEN
          l_line := 370;
          g_tab_arg(nvl(g_tab_arg.LAST,0)+1).arg_name := 'ARCHLOADFILEDIR';
          g_tab_arg(g_tab_arg.LAST).block_id          := l_block_id;
          g_tab_arg(g_tab_arg.LAST).arg_value         := substr(l_text,instr(l_text,'/',-1,2)+1,instr(l_text,'/',-1,1)-instr(l_text,'/',-1,2)-1);
        ELSIF(l_block_compress_logfile) AND (l_block_gzip_active) AND (l_text LIKE 'fi%') THEN
          l_line := 380;
          l_block_compress_logfile := FALSE;
          l_block_active           := FALSE;
          l_block_gzip_active      := FALSE;
        -- 16 statistics
        ELSIF(NOT l_block_active) AND (l_text LIKE 'echo "execute updp_statistics%') THEN
          l_line := 390;
          l_block_id               := l_block_statistics_id;
          --
          l_new_block_tab_id := nvl(g_tab_blocks.COUNT,0)+1;
          g_tab_blocks(l_new_block_tab_id).block_id := l_block_id;
          g_tab_blocks(l_new_block_tab_id).order_no := l_new_block_tab_id;
          --
          IF NOT(l_table_name_stored) THEN
            l_line := 400;
            g_tab_arg(nvl(g_tab_arg.LAST,0)+1).arg_name := 'TABLENAME';
            g_tab_arg(g_tab_arg.LAST).block_id          := l_block_id;
            g_tab_arg(g_tab_arg.LAST).arg_value         := substr(l_text,instr(l_text,'(',-1,1)+2,instr(l_text,')',-1,1)-instr(l_text,'(',-1,1)-2);
            l_table_name_stored                         := TRUE;
          END IF;
        -- 17 check return value
        ELSIF(NOT l_block_active) AND (l_text LIKE 'if [ $dwh_ret -eq 0 ]%') THEN
          l_line := 410;
          l_block_chk_return       := TRUE;
          l_block_active           := TRUE;
          l_block_id               := l_block_chk_return_id;
          --
          l_new_block_tab_id := nvl(g_tab_blocks.COUNT,0)+1;
          g_tab_blocks(l_new_block_tab_id).block_id := l_block_id;
          g_tab_blocks(l_new_block_tab_id).order_no := l_new_block_tab_id;
          --
        ELSIF(l_block_chk_return) AND (l_text LIKE 'fi%') THEN
          l_line := 420;
          l_block_chk_return       := FALSE;
          l_block_active           := FALSE;
        -- 18 return
        ELSIF(NOT l_block_active) AND (l_text LIKE 'return $dwh_ret%') THEN
          l_line := 430;
          l_block_id               := l_block_return_id;
          --
          l_new_block_tab_id := nvl(g_tab_blocks.COUNT,0)+1;
          g_tab_blocks(l_new_block_tab_id).block_id := l_block_id;
          g_tab_blocks(l_new_block_tab_id).order_no := l_new_block_tab_id;
          --
        ELSIF(NOT l_block_active) AND (l_text LIKE 'ln -s $DATAPATH/incoming/$name%') THEN
          l_line := 440;
          l_block_id               := l_block_ln_id;
          --
          l_new_block_tab_id := nvl(g_tab_blocks.COUNT,0)+1;
          g_tab_blocks(l_new_block_tab_id).block_id := l_block_id;
          g_tab_blocks(l_new_block_tab_id).order_no := l_new_block_tab_id;
          --
          IF NOT(l_loadfile_stored) THEN
            l_line := 450;
            g_tab_arg(nvl(g_tab_arg.LAST,0)+1).arg_name := 'LOADFILE';
            g_tab_arg(g_tab_arg.LAST).block_id          := l_block_id;
            g_tab_arg(g_tab_arg.LAST).arg_value         := substr(l_text,instr(l_text,'/',-1,1)+1);
            l_loadfile_stored                           := TRUE;
          END IF;
        ELSIF(NOT l_block_active) AND (l_text LIKE 'dbload -d $DBNAME%$SQLLDR_MAXERR%') THEN
          l_line := 460;
          l_block_dbload_para      := TRUE;
          l_block_active           := TRUE;
          l_block_id               := l_block_dbload_para_id;
          --
          l_new_block_tab_id := nvl(g_tab_blocks.COUNT,0)+1;
          g_tab_blocks(l_new_block_tab_id).block_id := l_block_id;
          g_tab_blocks(l_new_block_tab_id).order_no := l_new_block_tab_id;
          --
          IF NOT(l_comfile_stored) THEN
           l_line := 470;
            g_tab_arg(nvl(g_tab_arg.LAST,0)+1).arg_name := 'COMFILE';
            g_tab_arg(g_tab_arg.LAST).block_id          := l_block_id;
            g_tab_arg(g_tab_arg.LAST).arg_value         := substr(l_text,instr(l_text,'/',-1,1)+1,instr(l_text,' ',instr(l_text,'/',-1,1)+1,1)-instr(l_text,'/',-1,1));
            l_comfile_stored                            := TRUE;
          END IF;
        ELSIF(l_block_dbload_para) AND (l_text LIKE 'rc=$%') THEN
          l_line := 480;
          l_block_dbload_para                           := FALSE;
          l_block_chk_dbload                            := TRUE;
          l_block_active                                := TRUE;
          l_block_id                                    := l_block_chk_dbload_id;
          --
          l_new_block_tab_id := nvl(g_tab_blocks.COUNT,0)+1;
          g_tab_blocks(l_new_block_tab_id).block_id := l_block_id;
          g_tab_blocks(l_new_block_tab_id).order_no := l_new_block_tab_id;
          --
        ELSIF(NOT l_block_active) AND (l_text LIKE 'sqlldr $ORACLE_CONN%') THEN
          l_line := 490;
          l_block_sqlldr                                := TRUE;
          l_block_active                                := TRUE;
          l_block_id                                    := l_block_sqlldr_id;
          --
          l_new_block_tab_id := nvl(g_tab_blocks.COUNT,0)+1;
          g_tab_blocks(l_new_block_tab_id).block_id := l_block_id;
          g_tab_blocks(l_new_block_tab_id).order_no := l_new_block_tab_id;
          --
          g_tab_arg(nvl(g_tab_arg.LAST,0)+1).arg_name   := 'CONTROLFILE';
          g_tab_arg(g_tab_arg.LAST).block_id            := l_block_id;
          g_tab_arg(g_tab_arg.LAST).arg_value           := substr(l_text,instr(l_text,'/',-1,1)+1,instr(l_text,' ',instr(l_text,'/',-1,1)+1,1)-instr(l_text,'/',-1,1));
        ELSIF(l_block_sqlldr) AND (l_text LIKE 'rc=$%') THEN
          l_line := 500;
          l_block_sqlldr                                := FALSE;
          l_block_chk_dbload                            := TRUE;
          l_block_active                                := TRUE;
          l_block_id                                    := l_block_chk_dbload_id;
          --
          l_new_block_tab_id := nvl(g_tab_blocks.COUNT,0)+1;
          g_tab_blocks(l_new_block_tab_id).block_id := l_block_id;
          g_tab_blocks(l_new_block_tab_id).order_no := l_new_block_tab_id;
          --
        ELSIF(NOT l_block_active) AND (l_text LIKE '%Nachbehandlung%') THEN
          l_line := 510;
          l_block_nachbehandlung                        := TRUE;
          l_block_active                                := TRUE;
          l_block_id                                    := l_block_nachbehandlung_id;
          --
          l_new_block_tab_id := nvl(g_tab_blocks.COUNT,0)+1;
          g_tab_blocks(l_new_block_tab_id).block_id := l_block_id;
          g_tab_blocks(l_new_block_tab_id).order_no := l_new_block_tab_id;
          --
        ELSIF(l_block_nachbehandlung) AND (l_text LIKE '%exec%dbaccess%') THEN
          l_line := 520;
          g_tab_arg(nvl(g_tab_arg.LAST,0)+1).arg_name   := 'SCRIPTNACHBEHANDLUNG';
          g_tab_arg(g_tab_arg.LAST).block_id            := l_block_id;
          g_tab_arg(g_tab_arg.LAST).arg_value           := substr(l_text,instr(l_text,'exec ',1,1)+5,instr(l_text,'()',1,1)+2-instr(l_text,'exec',1,1)-5);
        ELSIF(l_block_nachbehandlung) AND (l_text LIKE '%if [ $? -ne 0 ]%') THEN
          l_line := 530;
          l_block_nachbehandlung                        := FALSE;
          l_block_chk_nachbehandlung                    := TRUE;
          l_block_id                                    := l_block_chk_nachbehandlung_id;
          --
          l_new_block_tab_id := nvl(g_tab_blocks.COUNT,0)+1;
          g_tab_blocks(l_new_block_tab_id).block_id := l_block_id;
          g_tab_blocks(l_new_block_tab_id).order_no := l_new_block_tab_id;
          --
        ELSIF(l_block_chk_nachbehandlung) AND (l_text LIKE '%fi%') THEN
          l_line := 540;
          l_block_chk_nachbehandlung                    := FALSE;
          l_block_active                                := FALSE;
        ELSIF(NOT l_block_active) AND (l_text LIKE '%echo "exec updp_statistics%') THEN
          l_line := 550;
          l_block_id                                    := l_block_statistics_short_id;
          --
          l_new_block_tab_id := nvl(g_tab_blocks.COUNT,0)+1;
          g_tab_blocks(l_new_block_tab_id).block_id := l_block_id;
          g_tab_blocks(l_new_block_tab_id).order_no := l_new_block_tab_id;
          --
          --
          IF NOT(l_table_name_stored) THEN
            l_line := 560;
            g_tab_arg(nvl(g_tab_arg.LAST,0)+1).arg_name := 'TABLENAME';
            g_tab_arg(g_tab_arg.LAST).block_id          := l_block_id;
            g_tab_arg(g_tab_arg.LAST).arg_value         := substr(l_text,instr(l_text,'(',-1,1)+2,instr(l_text,')',-1,1)-instr(l_text,'(',-1,1)-3);
            l_table_name_stored                         := TRUE;
          END IF;
        END IF;
      END LOOP;
      l_line := 570;
    EXCEPTION
      WHEN no_data_found THEN
        utl_file.fclose(l_file);
      WHEN others THEN
        dbms_output.put_line('Exception during reading file(' || to_char(l_line) || ': ' || SQLERRM);
    END;
    l_line := 580;
    IF(g_trace) THEN
      l_line := 590;
      i_file := g_tab_arg.FIRST;
      WHILE i_file IS NOT NULL LOOP
        dbms_output.put(g_tab_arg(i_file).arg_name || ': ');
        dbms_output.put_line(g_tab_arg(i_file).arg_value);
        i_file := g_tab_arg.NEXT(i_file);
      END LOOP;
      l_line := 600;
      i_file := g_tab_blocks.FIRST;
      WHILE i_file IS NOT NULL LOOP
        dbms_output.put_line('Block found: ' || g_tab_blocks(i_file).block_id);
        i_file := g_tab_blocks.NEXT(i_file);
      END LOOP;
    END IF;
    l_line := 610;
    utl_file.fclose(l_file);
    g_file_name := p_file_name;
    l_script_type := get_script_type_from_blocks;
    l_line := 620;
    IF(l_script_type = 0) THEN
      --IF(g_tab_blocks.COUNT = 0) THEN
      --  return 0;
      --END IF;
      l_line := 630;
      SELECT nvl(max(id),0)+1
        INTO l_new_block_group_id
        FROM block_groups
      ;
      IF(g_trace) THEN
        dbms_output.put_line('inserting new group ' || l_new_block_group_id);
      END IF;
      SELECT nvl(max(id),0)+1
        INTO l_new_block_group_block_id
        FROM block_group_blocks
      ;
      l_line := 640;
      INSERT INTO block_groups(id)
      VALUES(l_new_block_group_id)
      ;
      --
      l_line := 650;
      l_cnt := 1;
      i_file := g_tab_blocks.FIRST;
      WHILE i_file IS NOT NULL LOOP
        INSERT INTO block_group_blocks(id,block_group_id,block_id,order_no)
        VALUES(l_new_block_group_block_id,l_new_block_group_id,g_tab_blocks(i_file).block_id,l_cnt)
        ;
        i_file := g_tab_blocks.NEXT(i_file);
        l_new_block_group_block_id := l_new_block_group_block_id + 1;
        l_cnt := l_cnt + 1;
      END LOOP;
      l_line := 660;
      COMMIT;
      return l_new_block_group_id;
    ELSE
      IF(g_trace) THEN
        dbms_output.put_line('Script Type: ' || l_script_type);
      END IF;
    END IF;
    return l_script_type;
  EXCEPTION
    WHEN others THEN
      IF utl_file.is_open(file => l_file) THEN
        utl_file.fclose(l_file);
      END IF;
      dbms_output.put_line('Exception in get_script_type(' || p_file_name || ', line ' || to_char(l_line) || '): ' || SQLERRM);
      return -1;
  END get_script_type;

  PROCEDURE analyze_filelist
  IS
    i_file        BINARY_INTEGER;
    l_file_name   VARCHAR2(1000);
    l_script_type NUMBER;
    TYPE t_tab_cnt IS TABLE OF NUMBER INDEX BY BINARY_INTEGER;
    l_tab_cnt     t_tab_cnt;
    l_max_files   NUMBER := nvl(g_max_files,1000000);
    l_line        NUMBER;
    l_cnt         NUMBER;
  BEGIN
    i_file := g_filelist.FIRST;
    l_line := 10;
    WHILE i_file IS NOT NULL LOOP
      l_line := 20;
      l_file_name := g_filelist(i_file);
      EXIT WHEN l_max_files <= 0;
      l_line := 30;
      l_script_type := get_script_type(p_file_name => l_file_name);
      --
      IF(l_file_name <> 'atf_bewertung_bas.sh') THEN
        l_line := 40;
        --INSERT INTO scripts(id,name,block_group_id)
        --VALUES(i_file,l_file_name,l_script_type);
      END IF;
      l_line := 50;
      --
      IF(l_tab_cnt.EXISTS(l_script_type)) THEN
        l_line := 60;
        l_tab_cnt(l_script_type) := l_tab_cnt(l_script_type) + 1;
      ELSE
        l_line := 70;
        l_tab_cnt(l_script_type) := 1;
      END IF;
      l_line := 80;
      i_file := g_filelist.NEXT(i_file);
      l_max_files := l_max_files - 1;
    END LOOP;
    l_line := 90;
    --
    i_file := l_tab_cnt.FIRST;
    WHILE i_file IS NOT NULL LOOP
      l_cnt := i_file;
      dbms_output.put_line( 'Type ' || l_cnt || ': ' || to_char(l_tab_cnt(i_file)) || ' Files');
      i_file := l_tab_cnt.NEXT(i_file);
    END LOOP;
    l_line := 100;
    COMMIT;
  EXCEPTION
    WHEN others THEN
      dbms_output.put_line('Exception in analyze_filelist (' || l_file_name || ',' || l_line || '): ' || SQLERRM);
      raise;
  END analyze_filelist;

  PROCEDURE analyze_files
  IS
    l_directory_path VARCHAR2(1000);
    l_file           utl_file.file_type;
  BEGIN
    g_filelist.DELETE;
    SELECT directory_path INTO l_directory_path FROM all_directories WHERE directory_name = g_directory_name;
    l_file := open_file(p_directory_name => g_directory_name, p_file_name => g_filelist_file);
    read_filelist(p_file => l_file);
    analyze_filelist;
    IF utl_file.is_open(file => l_file) THEN
      close_file(l_file);
    END IF;
  EXCEPTION
    WHEN others THEN
      IF utl_file.is_open(file => l_file) THEN
        close_file(l_file);
      END IF;
      dbms_output.put_line('Exception in analyze_files: ' || SQLERRM);
  END analyze_files;

  PROCEDURE do_create(p_max_files IN NUMBER DEFAULT g_max_files)
  IS
    l_file_name              VARCHAR2(100);
    l_file                   utl_file.file_type;
    l_text                   VARCHAR2(1000);
    l_max_replace_iterations NUMBER := g_max_replace_iterations;
    i_file                   BINARY_INTEGER;
  BEGIN
    i_file := g_filelist.FIRST;
    WHILE i_file IS NOT NULL LOOP
      l_file_name := g_filelist(i_file);
      l_file := utl_file.fopen(location => g_directory_name, filename => l_file_name, open_mode => 'w', max_linesize => 32760);
      FOR r_scripts IN c_scripts(p_script_name => l_file_name) LOOP
        FOR r_text IN c_text(p_script_id => r_scripts.id) LOOP
          l_max_replace_iterations := 5;
          l_text := r_text.text;
          WHILE (instr(l_text,'%',1,1)>0) AND (l_max_replace_iterations > 0) LOOP
            FOR r_args IN c_args(p_script_id => r_scripts.id,p_block_id => r_text.block_id) LOOP
              l_text := replace(l_text,'%'||r_args.argument_name||'%',r_args.value);
            END LOOP;
            l_max_replace_iterations := l_max_replace_iterations - 1;
          END LOOP;
          utl_file.put_line(l_file,l_text);
        END LOOP;
      END LOOP;
      utl_file.fclose(l_file);
      i_file := g_filelist.NEXT(i_file);
    END LOOP; -- all files of tab
  EXCEPTION
    WHEN others THEN
      IF utl_file.is_open(file => l_file) THEN
        utl_file.fclose(l_file);
      END IF;
      dbms_output.put_line('Exception in do_create: ' || SQLERRM);
  END do_create;
BEGIN
  g_filelist.DELETE;
END create_scripts;