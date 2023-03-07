create or replace procedure spool_ddl (p_object_type in varchar2, p_owner in varchar2, p_object_name in varchar2, p_path in varchar2, p_version in number default null, p_file in varchar2 default null)
/*
  -- Projekt: ???
  -- Prozedurname: spool_ddl
  -- Version: 1.0
  -- Autor: Ivan Nikolov [IN]
  -- Funktion: Generierung von ddl-Statements fuer db-Objekte als Dateien
  -- Abhaengigkeiten / Bemerkungen:
  -- Aenderungshistorie (Wer, Was, Wann):
  -- Wann      Wer    Was
  -- _____________________________________________________________________________
  -- 20230306  IN     Ersterstellung
  -- 20230207  IN     Test
  --
  --
*/
is
  l_clob CLOB;
  l_filename VARCHAR2(255);
  l_dir      VARCHAR2(255);
  l_query    VARCHAR2(255);
  l_version  NUMBER;
begin

     BEGIN
       l_query := 'SELECT directory_name FROM ALL_DIRECTORIES WHERE LOWER(directory_path) = LOWER('''||p_path||''') AND rownum = 1';
       EXECUTE IMMEDIATE l_query INTO l_dir;
     EXCEPTION
       WHEN NO_DATA_FOUND THEN
        raise_application_error (-20002, 'No directory for '||p_path||' defined!');
     END;
     
    if p_version is null then
      select max(o.version) into l_version from object_changes_log o
       where upper(o.OWNER) = upper(p_owner)
         and upper(o.OBJECT_TYPE) = upper(p_object_type)
         and upper(o.OBJECT_NAME) = upper(p_object_name)
      ;
    else
      l_version := p_version;
    end if;
    
    if p_file is null then
      l_filename := lower(p_object_name)||'.v'||to_char(l_version)||'.sql';
    else
      l_filename := p_file;
    end if;

   select o.last_ddl into l_clob from object_changes_log o
   where upper(o.OWNER) = upper(p_owner)
   and upper(o.OBJECT_TYPE) = upper(p_object_type)
   and upper(o.OBJECT_NAME) = upper(p_object_name)
   and version = l_version
   ;

   dbms_xslprocessor.clob2file(l_clob,l_dir,l_filename, nls_charset_id('AL32UTF8'));

   EXCEPTION
    WHEN OTHERS THEN
      raise_application_error (-20001, 'Eror: '||SQLERRM);

end;