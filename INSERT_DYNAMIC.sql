


/* Solve table production issues */
select column_name from all_tab_columns
where  table_name = 'RAMESH1234';

set serveroutput on;
DECLARE 


v_columns varchar(30000) := '';
v_count number := 0;

v_column_count number := 151;
v_table_select_name varchar(40) := '';
v_condition varchar(100) := 'where fqn_id like ''%a''';

v_sql varchar(30000) := '';

BEGIN

    for i in(select column_name from all_tab_columns
            where  table_name = 'FIBERSPLICE')
    loop
        dbms_output.put_line(i.column_name);
        v_count := v_count + 1;
        v_columns := v_columns ||  i.column_name ||  case when v_count < v_column_count then  ',' end;
    end loop;
    
    v_sql := 'INSERT INTO FIBERCABLE (' || v_columns || ') SELECT '|| v_columns || ' FROM ' || v_table_select_name || ' ' || v_condition;
    dbms_output.put_line(v_sql);
    execute immediate v_sql;
    
    
END;

select * from fibercable where fqn_id like '%a';






