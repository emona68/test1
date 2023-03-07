create or replace PACKAGE BODY          "ARCHIVER" AS

   /** Exception for ORA-00942 */
   object_not_found exception;
   pragma exception_init (object_not_found, -942);

   /** Default date format */
   c_default_date_format constant varchar2(30) := 'DD.MM.YYYY HH24:MI:SS';
  
   /**
    * Procedure creates a new table defined by *dest* parameters and copies data from given source table (*src* params)
    * @param p_src_table Name of the source table.
    * @param p_src_owner Owner of the source table. Default is the owner of this package.
    * @param p_dest_table Name of the target table. Default is the name of the source table.
    * @param p_dest_owner Owner of the target table. Default is the owner of this package.
    * @param p_dest_tbsp Tablespace for the target table. If null, the table is created in the default tablespace of the owner of this package.
    * @param p_pct_free Value for the PCTFREE parameter for target table. Defaults to 0. PCTFREE defines how much space is reserved for data chages.
    * @param p_parallel Value for PARALLEL parameter of the table and parallel hint in SELECT statement. Defaults to NULL (auto degree of parallelism).
    *  {*} 1 serial run
    *  {*} null Degree of paralellism will be computed (AUTO), and can be 1 (serial run) or greater (parallel run).
    *  {*} 2-n 
    * @param p_nologging Determines nologging seting for the table. Defaults to true.
    *  {*} true Table is created with nologging clause.
    *  {*} false Table is created with logging clause.
    *  {*} null Table is created with logging clause.
    * @param p_compress Determines compress seting for the table. Defaults to true.
    *  {*} true Table is created with compress clause.
    *  {*} false Table is created with nocompress clause.
    *  {*} null Table is created with nocompress clause.
    * @param p_dest_tab_suffix Suffix used for target table. Default is '_ARC'.
    */
   procedure archive_table(
     p_src_table in user_tables.table_name%type,
     p_src_owner in user_users.username%type default user,
     p_dest_table in user_tables.table_name%type default null,
     p_dest_owner in user_users.username%type default user,
     p_dest_tbsp in user_tables.tablespace_name%type default null,
     p_pct_free in number default 0,
     p_parallel in number default null,
     p_nologging in boolean default true,
     p_compress in boolean default true,
     p_dest_tab_suffix in varchar2 default '_ARC'
     ) is 
    -- template for drop table
    l_template_drop varchar2(500) := 'drop table ${dest_owner}.${dest_table}${dest_tab_suffix} purge';   
    
    -- template for create table
    l_template varchar2(500) := 
    'create table ${dest_owner}.${dest_table}${dest_tab_suffix}
    ${dest_tbsp} 
    pctfree ${pct_free} 
    parallel ${parallel} 
    ${nologging}
    ${compress}
    as select /*+ parallel (${parallel_hint}) */ * from ${src_owner}.${src_table}';
    
    l_message varchar2(200) := 'Archiving table ${src_owner}.${src_table} to ${dest_owner}.${dest_table}${dest_tab_suffix}, start_time: ${start_time}';
    
    l_dest_table user_tables.table_name%type;
    l_parallel_hint varchar2(30);
  begin
    -- if dest table is not defined use the name of the source taable
    l_dest_table := nvl(p_dest_table, p_src_table);
    
    -- try to drop object
    begin 
      l_template_drop := replace(l_template_drop, '${dest_owner}', p_dest_owner);
      l_template_drop := replace(l_template_drop, '${dest_table}', l_dest_table);
      l_template_drop := replace(l_template_drop, '${dest_tab_suffix}', p_dest_tab_suffix);
      
      execute immediate l_template_drop;
    exception 
      when object_not_found then -- ignore if it doesn't exist
        null;
    end;     
    
    l_template := replace(l_template, '${dest_owner}', p_dest_owner);
    l_template := replace(l_template, '${dest_table}', l_dest_table);
    l_template := replace(l_template, '${dest_tab_suffix}', p_dest_tab_suffix);
    
    -- adding a tablespace parameter, if it was defined
    if p_dest_tbsp is null then
      l_template := replace(l_template, '${dest_tbsp}', null);      
    else 
      l_template := replace(l_template, '${dest_tbsp}', 'tablespace ' || p_dest_tbsp);
    end if;
    
    l_template := replace(l_template, '${pct_free}', p_pct_free);
    l_template := replace(l_template, '${parallel}', p_parallel);
        
    if p_parallel is null then 
      l_parallel_hint := 'AUTO';
    else
      l_parallel_hint := cast (p_parallel as varchar2);
    end if;
    
    l_template := replace(l_template, '${parallel_hint}', l_parallel_hint);
    
    l_template := replace(l_template, '${src_owner}', p_src_owner);
    l_template := replace(l_template, '${src_table}', p_src_table);
    
    if p_nologging then 
      l_template := replace(l_template, '${nologging}', 'nologging');
    else
      l_template := replace(l_template, '${nologging}', 'logging');
    end if;     
    
    if p_compress then 
      l_template := replace(l_template, '${compress}', 'compress');
    else
      l_template := replace(l_template, '${compress}', 'nocompress');    
    end if;

    l_message := replace(l_message, '${src_owner}', p_src_owner);
    l_message := replace(l_message, '${src_table}', p_src_table);
    l_message := replace(l_message, '${dest_owner}', p_dest_owner);
    l_message := replace(l_message, '${dest_table}', p_dest_table);
    l_message := replace(l_message, '${dest_tab_suffix}', p_dest_tab_suffix);
    l_message := replace(l_message, '${start_time}', to_char(sysdate, c_default_date_format));
    
         
    dbms_output.put_line(l_message);
    
    begin 
      execute immediate l_template;   
    exception 
      when object_not_found then
        raise_application_error(-20001, 'Source or destination table not found', TRUE);
    end;
  end archive_table;

  /**
   * Procedure itaretes through admin_archive table and archives the configured tables.
   */
  procedure archive_tables is 
    cursor tables_to_archive_cur is
      select aa.* 
        from admin_archive aa 
       where aa.enabled = 'Y' 
         and nvl(aa.last_archived, date '2000-01-01') + aa.interval_ym + aa.interval_ds < sysdate;
    
    type config_tab_type is table of tables_to_archive_cur%rowtype;
   
    l_configs config_tab_type;
  begin   
    
    open tables_to_archive_cur;
    fetch tables_to_archive_cur bulk collect into l_configs;
    close tables_to_archive_cur;

    for i in 1..l_configs.count loop

      archive_table(
        p_src_table => l_configs(i).src_table,
        p_src_owner => l_configs(i).src_owner,
        p_dest_table => l_configs(i).dest_table,
        p_dest_owner => l_configs(i).dest_owner,
        p_dest_tbsp => l_configs(i).dest_tbsp,
        p_pct_free => l_configs(i).pct_free,
        p_parallel => l_configs(i).parallel,
        p_nologging => case l_configs(i).nologging when 'Y' then true else false end,
        p_compress => case l_configs(i).compress_dest when 'Y' then true else false end,
        p_dest_tab_suffix => l_configs(i).dest_tab_suffix);

      update admin_archive aa
      set aa.last_archived = sysdate
      where aa.id = l_configs(i).id;
     
      commit;     
    end loop;
    
    dbms_output.put_line('Process successfully completed at :' || to_char(sysdate, c_default_date_format));
    
  end archive_tables;

END archiver;