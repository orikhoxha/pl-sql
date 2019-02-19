
/* Gather data from all  backup tables. Create the dynamic sql */
set serveroutput on
declare

    V_MAX_BUFFER_VARCHAR constant number := 32767;
    
    v_max_cols_row NUMBER := 10;

    v_main_table constant varchar(30) := 'SPLICECLOSURE';
    v_columns varchar2(32767) := '';
    v_columns_count NUMBER := 0;
    v_tables_count NUMBER := 0;
    v_cast_data_type VARCHAR2(30) := '';
    
    v_len_sql number := 0;
    v_col_data_type varchar2(30) := '';
    
    v_structure_query_columns varchar2(32767) := '';
    
    /* Save the sql-s in the array */
    type v_sql_queries_typ is table of varchar2(32767) index by BINARY_INTEGER;
    v_indx_typ BINARY_INTEGER := 0;
    
    v_sql_list v_sql_queries_typ;
    
    v_buffer_length NUMBER := 0;
    
begin
    
    /* Get the count of columns from the main table */
    select count(*)
    into v_columns_count 
    from all_tab_columns where table_name = v_main_table;
    
    /* Get the count of table backups */
    select count(*)
    into v_tables_count
    from all_tables 
    where table_name like (v_main_table || '_%');
    
    
    /* Structure query columns */
    for j in( select column_name, data_type, rownum from all_tab_columns where  table_name = UPPER(v_main_table))
    loop
         v_structure_query_columns := v_structure_query_columns || j.column_name || case when (j.rownum < v_columns_count and j.rownum > v_max_cols_row and j.rownum mod v_max_cols_row = 0) then ', ' || chr(10) when j.rownum < v_columns_count then ','  else ' ' end;   
    end loop;
    
    v_structure_query_columns := 'select ' || v_structure_query_columns || ' from ' || v_main_table || ' where 1 = 0 ' || ' ' || chr(10)  || 'union all' || chr(10);
    
    
    v_sql_list(0) := v_structure_query_columns;
    
    /*
        Get all the columns for table fibersplice
        Check if each of the cable has that
        if yes( then colums = column, else null 
    */
    for i in (select table_name, rownum from all_tables where table_name  like (v_main_table || '_%') ORDER BY TABLE_NAME)
    loop
            
        for j in( select column_name, data_type, rownum from all_tab_columns where  table_name = UPPER(v_main_table))
        loop
        
            /* Check if the table has that column, if yes the add, if not the null as i.tablename */
            for k in (select count(*) count_col from all_tab_columns where table_name = i.table_name and column_name = j.column_name)
            loop
            
                begin
                    select data_type
                    into v_col_data_type
                    from all_tab_columns
                    where table_name = i.table_name and column_name = j.column_name;
                    
                    exception 
                        when NO_DATA_FOUND THEN
                            v_col_data_type := '';
                end;

                if(k.count_col = 0) then
                    v_columns := v_columns || 'null';
                else
                    if (j.data_type <> v_col_data_type) then
                        case j.data_type
                            when 'NVARCHAR2' THEN v_cast_data_type := 'to_nchar';
                            when 'CHAR' then v_cast_data_type := 'to_char';
                            when 'NUMBER' then v_cast_data_type := 'to_number';
                            else v_cast_data_type := '';
                        end case; 
                        
                        v_columns := v_columns || v_cast_data_type || '(' || j.column_name || ')';
                    else 
                        v_columns := v_columns || j.column_name;
                    end if;
                end if;

                v_columns := v_columns || case when (j.rownum < v_columns_count and j.rownum > v_max_cols_row and j.rownum mod v_max_cols_row = 0 ) then (', ' || chr(10)) when j.rownum < v_columns_count then ','  else ' ' end; 
            end loop;
        end loop;
        
        /* If the length is > 30000 create new varchar2, append the data to it and continue  */
        
        /* Initialize the list at position n */
        
        if((length(v_sql_list(v_indx_typ)) + length(v_columns)) > V_MAX_BUFFER_VARCHAR) then
            
            v_indx_typ := v_indx_typ + 1;
            v_sql_list(v_indx_typ) := '';
        end if;
        
        v_sql_list(v_indx_typ) := v_sql_list(v_indx_typ) || 'select ' || v_columns || ' from ' || i.table_name || case when i.rownum < v_tables_count then (' ' || chr(10)  || 'union all' || chr(10)) end;
         
        v_buffer_length :=  v_buffer_length + length(v_sql_list(v_indx_typ)) + length(v_columns);
        
        /* Free the v_column for the next table */
        v_columns := '';    
    end loop;
        
    
    for i in 0..v_sql_list.last
    loop
        dbms_output.put_line(v_sql_list(i));
    end loop;
end;