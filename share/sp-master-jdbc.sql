create or replace function addToJDBCRC( varchar, varchar ) returns integer as '
declare
  myid bigint := -1;
  mylfn alias for $1;
  mypfn alias for $2;
  fake rc_attr%ROWTYPE;
begin
  select into myid id from rc_lfn where lfn=mylfn and pfn=mypfn;
  if not found then
    insert into rc_lfn(lfn,pfn) values(mylfn,mypfn);
    select into myid id from rc_lfn where lfn=mylfn and pfn=mypfn;
  end if;
  select into fake * from rc_attr where id=myid and name=''pool'' and value=''local'';
  if not found then
    insert into rc_attr values(myid,''pool'',''local'');
  end if;
  return myid;
end;
' language plpgsql;
