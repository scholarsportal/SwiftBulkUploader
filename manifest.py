  def _upload_object_job(self, conn, container, source, obj, options,
                           results_queue=None):
        res = {
            'action': 'upload_object',
            'container': container,
            'object': obj
        }
        if hasattr(source, 'read'):
            stream = source
            path = None
        else:
            path = source
        res['path'] = path
        try:
            if obj.startswith('./') or obj.startswith('.\\'):
                obj = obj[2:]
            if obj.startswith('/'):
                obj = obj[1:]
            if path is not None:
                put_headers = {'x-object-meta-mtime': "%f" % getmtime(path)}
            else:
                put_headers = {'x-object-meta-mtime': "%f" % round(time())}

            res['headers'] = put_headers

            # We need to HEAD all objects now in case we're overwriting a
            # manifest object and need to delete the old segments
            # ourselves.
            old_manifest = None
            old_slo_manifest_paths = []
            new_slo_manifest_paths = set()
            if options['changed'] or options['skip_identical'] \
                    or not options['leave_segments']:
                checksum = None
                if options['skip_identical']:
                    try:
                        fp = open(path, 'rb')
                    except IOError:
                        pass
                    else:
                        with fp:
                            md5sum = md5()
                            while True:
                                data = fp.read(65536)
                                if not data:
                                    break
                                md5sum.update(data)
                        checksum = md5sum.hexdigest()
                try:
                    headers = conn.head_object(container, obj)
                    if options['skip_identical'] and checksum is not None:
                        if checksum == headers.get('etag'):
                            res.update({
                                'success': True,
                                'status': 'skipped-identical'
                            })
                            return res
                    cl = int(headers.get('content-length'))
                    mt = headers.get('x-object-meta-mtime')
                    if path is not None and options['changed']\
                            and cl == getsize(path) and \
                            mt == put_headers['x-object-meta-mtime']:
                        res.update({
                            'success': True,
                            'status': 'skipped-changed'
                        })
                        return res
                    if not options['leave_segments']:
                        old_manifest = headers.get('x-object-manifest')
                        if config_true_value(
                                headers.get('x-static-large-object')):
                            headers, manifest_data = conn.get_object(
                                container, obj,
                                query_string='multipart-manifest=get'
                            )
                            for old_seg in json.loads(manifest_data):
                                seg_path = old_seg['name'].lstrip('/')
                                if isinstance(seg_path, text_type):
                                    seg_path = seg_path.encode('utf-8')
                                old_slo_manifest_paths.append(seg_path)
                except ClientException as err:
                    if err.http_status != 404:
                        res.update({
                            'success': False,
                            'error': err
                        })
                        return res

            # Merge the command line header options to the put_headers
            put_headers.update(split_headers(options['header'], ''))

            # Don't do segment job if object is not big enough, and never do
            # a segment job if we're reading from a stream - we may fail if we
            # go over the single object limit, but this gives us a nice way
            # to create objects from memory
            if path is not None and options['segment_size'] and \
                    getsize(path) > int(options['segment_size']):
                res['large_object'] = True
                seg_container = container + '_segments'
                if options['segment_container']:
                    seg_container = options['segment_container']
                full_size = getsize(path)

                segment_futures = []
                segment_pool = self.thread_manager.segment_pool
                segment = 0
                segment_start = 0

                while segment_start < full_size:
                    segment_size = int(options['segment_size'])
                    if segment_start + segment_size > full_size:
                        segment_size = full_size - segment_start
                    if options['use_slo']:
                        segment_name = '%s/slo/%s/%s/%s/%08d' % (
                            obj, put_headers['x-object-meta-mtime'],
                            full_size, options['segment_size'], segment
                        )
                    else:
                        segment_name = '%s/%s/%s/%s/%08d' % (
                            obj, put_headers['x-object-meta-mtime'],
                            full_size, options['segment_size'], segment
                        )
                    seg = segment_pool.submit(
                        self._upload_segment_job, path, container,
                        segment_name, segment_start, segment_size, segment,
                        obj, options, results_queue=results_queue
                    )
                    segment_futures.append(seg)
                    segment += 1
                    segment_start += segment_size

                segment_results = []
                errors = False
                exceptions = []
                for f in interruptable_as_completed(segment_futures):
                    try:
                        r = f.result()
                        if not r['success']:
                            errors = True
                        segment_results.append(r)
                    except Exception as e:
                        errors = True
                        exceptions.append(e)
                if errors:
                    err = ClientException(
                        'Aborting manifest creation '
                        'because not all segments could be uploaded. %s/%s'
                        % (container, obj))
                    res.update({
                        'success': False,
                        'error': err,
                        'exceptions': exceptions,
                        'segment_results': segment_results
                    })
                    return res

                res['segment_results'] = segment_results

                if options['use_slo']:
                    segment_results.sort(key=lambda di: di['segment_index'])
                    for seg in segment_results:
                        seg_loc = seg['segment_location'].lstrip('/')
                        if isinstance(seg_loc, text_type):
                            seg_loc = seg_loc.encode('utf-8')
                        new_slo_manifest_paths.add(seg_loc)

                    manifest_data = json.dumps([
                        {
                            'path': d['segment_location'],
                            'etag': d['segment_etag'],
                            'size_bytes': d['segment_size']
                        } for d in segment_results
                    ])

                    put_headers['x-static-large-object'] = 'true'
                    mr = {}
                    conn.put_object(
                        container, obj, manifest_data,
                        headers=put_headers,
                        query_string='multipart-manifest=put',
                        response_dict=mr
                    )
                    res['manifest_response_dict'] = mr
                else:
                    new_object_manifest = '%s/%s/%s/%s/%s/' % (
                        quote(seg_container), quote(obj),
                        put_headers['x-object-meta-mtime'], full_size,
                        options['segment_size'])
                    if old_manifest and old_manifest.rstrip('/') == \
                            new_object_manifest.rstrip('/'):
                        old_manifest = None
                    put_headers['x-object-manifest'] = new_object_manifest
                    mr = {}
                    conn.put_object(
                        container, obj, '', content_length=0,
                        headers=put_headers,
                        response_dict=mr
                    )
                    res['manifest_response_dict'] = mr
            else:
                res['large_object'] = False
                if path is not None:
                    obr = {}
                    conn.put_object(
                        container, obj, open(path, 'rb'),
                        content_length=getsize(path), headers=put_headers,
                        response_dict=obr
                    )
                    res['response_dict'] = obr
                else:
                    obr = {}
                    conn.put_object(
                        container, obj, stream, headers=put_headers,
                        response_dict=obr
                    )
                    res['response_dict'] = obr
            if old_manifest or old_slo_manifest_paths:
                if old_manifest:
                    scontainer, sprefix = old_manifest.split('/', 1)
                    scontainer = unquote(scontainer)
                    sprefix = unquote(sprefix).rstrip('/') + '/'
                    delobjs = []
                    for delobj in conn.get_container(scontainer,
                                                     prefix=sprefix)[1]:
                        delobjs.append(delobj['name'])
                    drs = []
                    for dr in self.delete(container=scontainer,
                                          objects=delobjs):
                        drs.append(dr)
                    res['segment_delete_results'] = drs
                if old_slo_manifest_paths:
                    delobjsmap = {}
                    for seg_to_delete in old_slo_manifest_paths:
                        if seg_to_delete in new_slo_manifest_paths:
                            continue
                        scont, sobj = \
                            seg_to_delete.split('/', 1)
                        delobjs_cont = delobjsmap.get(scont, [])
                        delobjs_cont.append(sobj)
                        drs = []
                        for (dscont, dsobjs) in delobjsmap.items():
                            for dr in self.delete(container=dscont,
                                                  objects=dsobjs):
                                drs.append(dr)
                        res['segment_delete_results'] = drs

            # return dict for printing
            res.update({
                'success': True,
                'status': 'uploaded',
                'attempts': conn.attempts})
            return res

        except OSError as err:
            if err.errno == ENOENT:
                err = SwiftError('Local file %r not found' % path)
            res.update({
                'success': False,
                'error': err
            })
        except Exception as err:
            res.update({
                'success': False,
                'error': err
            })
        return res