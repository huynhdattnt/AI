def generate_image(pdf_file, num_pages, dir, need_pre_process_image=True):
    '''
    Generate 2 PNG files per PDF (1 for thumbnail and 1 regular size)
    :param pdf_file:
    :param dir:
    :return:
        - 1: success
        - 0: fail
    '''
    try:
        if num_pages:
            png_dir = dir + '/' + 'png'
            if not os.path.exists(png_dir):
                os.makedirs(png_dir)
            t = threading.Thread(target=_generate_image, name=str('hight_image'),
                             args=(pdf_file, png_dir))
            t.start()
            if need_pre_process_image:
                temp_dir = dir + '/' + 'temp'
                if not os.path.exists(temp_dir):
                    os.makedirs(temp_dir)
                t = threading.Thread(target=_generate_image, name=str('temp_image'),
                                     args=(pdf_file, temp_dir))
                t.start()
        else:
            logging.error('Pdf file invalid, number of pages equal 0')
            print 'Pdf file invalid, number of pages equal 0'

    except Exception as error:
        traceback.print_exc()

def _generate_image(pdf_file, dir):
    '''
    Implement generate images
    :param pdf_file: the path of pdf file
    :param dir: the directory which store iamges
    :return: None
    '''
    t_name = threading.currentThread().getName()
    pdf_name = os.path.basename(pdf_file).rsplit('.pdf', 1)[0]
    if t_name == 'temp_image':
        # generate temp images
        # /project/fbone/fbone/fbone/lib/ghostscript/bin/gs -dSAFER -dBATCH -dNOPAUSE -sDEVICE=pnggray -dINTERPOLATE -r300 -dDownScaleFactor=2 -sOutputFile=out2%d.png 1d63bab297c5bb9f9c4a4f36e10d18_1491734332.pdf -c 30000000
        execute_not_wait([
            current_app.conf.get("GHOST_SCRIPT"), '-dSAFER', '-dBATCH', '-dNOPAUSE', '-sDEVICE=pnggray', '-dINTERPOLATE',
            '-r300', '-dPDFSETTINGS=/prepress', '-dPDFFitPage', '-dDownScaleFactor=2',
            '-sOutputFile={}/{}_%06d.png'.format(dir, pdf_name), '-dUseTrimBox=true', '-dUseCropBox=true', '-f', str(pdf_file),
            '-c', '{}'.format(current_app.conf.get("GHOST_MEMORY")),
        ])
    else:
        #generate hight images
        #/project/fbone/fbone/fbone/lib/ghostscript/bin/gs -dSAFER -dBATCH -dNOPAUSE -sDEVICE=pnggray -r200 -dDownScaleFactor=2 -sOutputFile=out1%d.png 1d63bab297c5bb9f9c4a4f36e10d18_1491734332.pdf -c 30000000 setvmthreshold -f
        execute_not_wait([
            current_app.conf.get("GHOST_SCRIPT"),'-dSAFER', '-dBATCH', '-dNOPAUSE', '-sDEVICE=pnggray',
            '-r200', '-dPDFSETTINGS=/prepress','-dDownScaleFactor=2',
            '-sOutputFile={}/{}_%06d.png'.format(dir, pdf_name), '-f', str(pdf_file),
            '-c', '{}'.format(current_app.conf.get("GHOST_MEMORY")),
        ])

def ocr_pdf(s, pool, pdf_file, num_pages, dir, xml_data, reocr_pages=None):
    '''
    # Generate 2 PNG files per PDF (1 for thumbnail and 1 regular size)
    # Generate searchable PDF for each of images
    # And generate textfile Sphinx format
    :param pdf_file:
    :param dir:
    :return: 1519408
    '''
    try:
        if num_pages:
            pdf_name = os.path.basename(pdf_file).rsplit('.pdf', 1)[0]
            sub_path = '{}/{}/'.format(
                xml_data['box_number'],
                xml_data['box_part']
            )
            pages = []
            if reocr_pages:
                #run ocr again if the error has occurred
                for i, val in enumerate(reocr_pages):
                    while (len([name for name in os.listdir(dir) if
                                os.path.isfile(os.path.join(dir, name))]) <= val - current_app.conf.get(
                            "THREAD_NUM") and i >= current_app.conf.get(
                            "THREAD_NUM")):  # include png folder, temp folder, and pdf file
                        time.sleep(0.3)
                    t = threading.Thread(target=_ocr_pdf, name=str(val),
                                         args=(s, pool, pdf_name, dir))
                    t.daemon = True
                    t.start()

            else:
                for i in range(num_pages):
                    while (len([name for name in os.listdir(dir) if os.path.isfile(os.path.join(dir, name))]) <= i + 1 - current_app.conf.get("THREAD_NUM") and i >= current_app.conf.get("THREAD_NUM")): #include png folder, temp folder, and pdf file
                        time.sleep(0.3)
                    t = threading.Thread(target=_ocr_pdf, name=str(i + 1),
                                         args=(s, pool, pdf_name, dir))
                    t.daemon = True
                    t.start()
                    # pages.append({
                    #     'path': sub_path + '{}_{}.pdf'.format(pdf_name, format(int(i + 1), "06")),
                    #     'version': str(i + 1)
                    # })
            for i in range(num_pages):
                pages.append({
                    'path': sub_path + '{}_{}.pdf'.format(pdf_name, format(int(i + 1), "06")),
                    'version': str(i + 1)
                })

            #generate xml file
            generate_xml(dir, xml_data, num_pages)

            # Call to AddPages API
            data_json = {
                'bookmark_process_queue_id' : str(xml_data['bookmark_id']),
                'pages'     : pages,
            }
            call_to_esd_api('add_pages', 1, data_json)

    except Exception as error:
        traceback.print_exc()
    return 1

def _ocr_pdf(s, pool, pdf_name, dir):
    '''
    Orc processing - Generate searchable pdf file by png file
    :param s: using for semaphore threads
    :param pool: using for semaphore threads
    :param pdf_name: name of pdf file
    :param dir:
    :return:
        - 0: failed
        - 1: success
    '''
    logging.debug('Waiting to join the pool')
    try:
        with s:
            t_name = threading.currentThread().getName()
            pool.makeActive(t_name)

            temp_image_path     = '{}/temp/{}_{}.png'.format(dir, pdf_name, format(int(t_name), "06"))
            searchable_pdf_path = '{}/{}_{}.pdf'.format(dir, pdf_name, format(int(t_name), "06"))

            # Check and wait until a file exists to read it
            _time = 0
            while not os.path.exists(temp_image_path):
                _time = _time + 1
                if _time == current_app.conf.get("TIMEOUT"):
                    return 0 #timeout
                time.sleep(_time)
            # Following by: https://tpgit.github.io/Leptonica/readfile_8c_source.html
            if os.stat(temp_image_path).st_size < 12:
                time.sleep(1)

            logging.debug('Ocring pdf file is starting ... File path: %s' % searchable_pdf_path)
            # Ocring searchable pdf by temp image
            Ocr.set_up()  # one time setup
            ocrEngine = Ocr()
            ocrEngine.start_engine("eng")
            s = ocrEngine.recognize(temp_image_path, -1, -1, -1, -1, -1,
                                    OCR_RECOGNIZE_TYPE_TEXT, OCR_OUTPUT_FORMAT_PDF,
                                    PROP_PDF_OUTPUT_FILE=searchable_pdf_path, PROP_PDF_OUTPUT_RETURN_TEXT='text',
                                    ROCESS_TYPE="custom", PROP_IMG_PREPROCESS_CUSTOM_CMDS="scale(2);default()",
                                    PROP_PDF_OUTPUT_TEXT_VISIBLE=False)
            ocrEngine.stop_engine()

    except Exception as error:
        traceback.print_exc()
        return 0
    return 1

@signals.worker_process_init.connect
def worker_process_init(**kwargs):
    '''
    If ocr process have been failed (signal 6, 11), this function will run ocr again.
    :param kwargs:
    :return:
        - None
    '''
    logging.info('Re-OCR is starting again ...................... ')
    try:
        non_bulk_dir = current_app.conf.get("TMP_DIR_NON_BULK")
        dirs = os.walk(non_bulk_dir)
        for item in dirs:
            #read data_xml from this folder
            print 'item is: {}'.format(item)
            if len(item[1]):
                sub_folders = item[1] #item[1][0]
                for sub_folder in sub_folders:
                    xml_data_path = non_bulk_dir + '/{}/{}'.format(sub_folder, 'xml_data.txt')
                    temp_path = non_bulk_dir + '/{}/{}'.format(sub_folder, 'temp')
                    xml_data = None
                    if os.path.exists(xml_data_path):
                        with open(xml_data_path, 'rb') as f:
                            xml_data = json.loads(f.read().replace('\'', '"'))
                    if xml_data and os.path.exists(temp_path):
                        # Orcing pdf file again
                        pdf_file = non_bulk_dir + '/{}/{}.pdf'.format(sub_folder, xml_data['pdf_name'])
                        num_pages = 0
                        with open(pdf_file, 'rb') as f:
                            pdf_input = PdfFileReader(f, strict=False)
                            num_pages = pdf_input.getNumPages()
                        if num_pages:
                            for i in range(num_pages):
                                searchable_pdf = non_bulk_dir + '/{}/{}_{}.pdf'.format(sub_folder,xml_data['pdf_name'], format(i + 1, "06"))
                                print 'searchable pdf is: {}'.format(searchable_pdf)
                                if os.path.exists(searchable_pdf):
                                    continue
                                else:
                                    pool = ThreadPool()
                                    s = threading.Semaphore(current_app.conf.get("THREAD_NUM"))
                                    dir = non_bulk_dir + '/{}'.format(sub_folder)
                                    ocr_pdf(s, pool, pdf_file, num_pages, dir, xml_data, reocr_pages=range(i + 1, num_pages + 1))
                                    sync_and_update_bpq_as_processed(pool, pdf_file, xml_data, dir, num_pages,True)
                                    break
            break
    except Exception as error:
        traceback.print_exc()
        return 0
    return 1
