function hclear(link, hname, verbose=true){
	ret = 0;
	r = link.request('hclear', [hname]);
	try{
		ret = r.data;
	}catch(Exception e){
	}
	return ret;
}

function zclear(link, zname, verbose=true){
	ret = 0;
	r = link.request('zclear', [zname]);
	try{
		ret = r.data;
	}catch(Exception e){
	}
	return ret;
}

function qclear(link, zname, verbose=true){
	ret = 0;
	r = link.request('qclear', [zname]);
	try{
		ret = r.data;
	}catch(Exception e){
	}
	return ret;
}

function hlist_names(rows){
	names = [];
	i = 0;
	while(i < len(rows)){
		name = rows[i];
		i += 1;
		if(i >= len(rows)){
			break;
		}
		cnt = int(rows[i]);
		i += 1;
		skip = cnt * 2;
		if(skip < 0 || (i + skip > len(rows))){
			break;
		}
		i += skip;
		names.append(name);
	}
	return names;
}

function flushdb(link, data_type){
	resp = link.request('info', ['replication']);
	for(i=1; i<len(resp.data); i+=2){
		if(resp.data[i] == 'replication'){
			throw new Exception('flushdb is not allowed when replication is in use!');
		}
	}

	printf('\n');
	printf('============================ DANGER! ============================\n');
	printf('This operation is DANGEROUS and is not recoverable, if you\n');
	printf('really want to flush the whole db(delete ALL data in ssdb server),\n');
	printf('input \'yes\' and press Enter, or just press Enter to cancel\n');
	printf('\n');
	printf('flushdb will break replication states, you must fully understand\n');
	printf('the RISK before you doing this!\n');
	printf('\n');
	printf('> flushdb? ');
	
	line = sys.stdin.readline().strip();
	if(line != 'yes'){
		printf('Operation cancelled.\n\n');
		return;
	}

	printf('Begin to flushdb...\n');

	resp = link.request('flushdb');
	if(resp.code != 'ok' && resp.code != 'client_error'){
		throw new Exception(resp.message);
	}
	printf('compacting...\n');
	link.request('compact', ['all']);
	printf('done.\n');
	printf('\n');
}
