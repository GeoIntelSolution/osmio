#从osm中导出highways和roads
* 从osm中把所有数据tags中有highway的选择出来
* 利用highways把其对应的nodesID选择出来
* 利用nodesid把所有站点选择出来
* 利用nodesid ，从way_nodes表中把以nodes为端点的ways选择出来
* 利用查询的names连接,形成路口数据

## hard points
* plpgsql的书写[website](https://www.cnblogs.com/lottu/p/7410941.html)
* 去重函数的引入[anyarray](https://github.com/JDBurnZ/postgresql-anyarray/blob/master/stable/anyarray_uniq.sql)
```sql
DROP TABLE IF EXISTS highways;
DROP TABLE IF EXISTS tempid;
DROP TABLE IF EXISTS stops;
DROP TABLE IF EXISTS tempname;
DROP TABLE IF EXISTS selected_stops;

DROP FUNCTION IF EXISTS filltempid();
DROP FUNCTION IF EXISTS filltempname();

CREATE TABLE public.highways (
   id int8 NOT NULL,
   name varchar(100),
   nodes _int8 NULL,
   linestring geometry NULL,
   CONSTRAINT pk_highways PRIMARY KEY (id)
);
insert into highways
select id,tags->'name' as name ,nodes,linestring from ways where linestring notnull and tags ? 'highway'  ;

CREATE INDEX idx_highways_linestring ON public.highways  using gist(linestring);

create table tempid(
   id int8 NOT NULL,
   CONSTRAINT pk_temp_id PRIMARY KEY (id)
);

create or replace function filltempid()
returns void
as '
declare
   var bigint;
   i record;
begin
   for i in select nodes from highways h loop
      foreach var in array i.nodes loop
--       raise notice 'id:%',var;
         insert into tempid(id) values (var) on conflict (id) do nothing ;
      end loop;
   end loop;
end;
' language plpgsql;

--fill table tempid;
select filltempid();


CREATE TABLE public.stops (
   id int8 NOT NULL,
   tags hstore NULL,
   geom geometry(POINT, 4326) NULL,
   CONSTRAINT pk_stops PRIMARY KEY (id)
);

insert into stops
select id,tags,geom from nodes n
where n.id in (
select id from tempid t2
);

create table tempname(
   id int8 NOT NULL,
   name text[],
   CONSTRAINT pk_tempName_id PRIMARY KEY (id)
);


DROP FUNCTION IF EXISTS anyarray_uniq(anyarray);
CREATE OR REPLACE FUNCTION anyarray_uniq(with_array anyarray)
	RETURNS anyarray AS
'
	DECLARE
		-- The variable used to track iteration over "with_array".
		loop_offset integer;

		-- The array to be returned by this function.
		return_array with_array%TYPE := ''{}'';
	BEGIN
		IF with_array IS NULL THEN
			return NULL;
		END IF;
		
		IF with_array = ''{}'' THEN
		    return return_array;
		END IF;

		-- Iterate over each element in "concat_array".
		FOR loop_offset IN ARRAY_LOWER(with_array, 1)..ARRAY_UPPER(with_array, 1) LOOP
			IF with_array[loop_offset] IS NULL THEN
				IF NOT EXISTS(
					SELECT 1 
					FROM UNNEST(return_array) AS s(a)
					WHERE a IS NULL
				) THEN
					return_array = ARRAY_APPEND(return_array, with_array[loop_offset]);
				END IF;
			-- When an array contains a NULL value, ANY() returns NULL instead of FALSE...
			ELSEIF NOT(with_array[loop_offset] = ANY(return_array)) OR NOT(NULL IS DISTINCT FROM (with_array[loop_offset] = ANY(return_array))) THEN
				return_array = ARRAY_APPEND(return_array, with_array[loop_offset]);
			END IF;
		END LOOP;

	RETURN return_array;
 END;
' LANGUAGE plpgsql;

create or replace function filltempname()
returns void
as '
declare
   i record;
    j record;
    names text[];
begin
   for i in select id from stops loop
--    raise notice '%',i;
       names :=''{}'';
      for j in select name from highways w2 where w2.id in(select wn.way_id from way_nodes wn  where wn.node_id =i.id) loop
--       raise notice '%',j.name;
         if j.name  notnull then
            names := (select array_append(names,j.name::text));
         end if;
      end loop;
--    raise notice '%',names;
      if array_length(names,1)!=0 then
          insert into tempname(id,name) values(i.id,anyarray_uniq(names)) on conflict(id) do nothing ;
       end if;
   end loop;
end;
'language plpgsql;

select filltempname();

CREATE TABLE public.selected_stops (
   id int8 NOT NULL,
   name text[],
   tags hstore NULL,
   geom geometry(POINT, 4326) NULL,
   CONSTRAINT pk_selected_stops PRIMARY KEY (id)
);
insert into selected_stops
(select s.id,t.name,s.tags,s.geom from stops s,
tempname t
where s.id=t.id);


CREATE INDEX idx_selected_stops_geom ON public.selected_stops USING gist (geom);
```