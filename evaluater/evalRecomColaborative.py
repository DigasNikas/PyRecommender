"""
Version: 0.4

Written by Pedro Santos

Change log:
        - num_recom is received as input argument (Ehsan.Nia 22-08-2016)

"""
import argparse
import datetime
import os
import re

try:
    import boto3
    flag_b = True
except ImportError:
    flag_b = False

lst_in = []


def cleanlstin():
    global lst_in
    lst_in = []


def cleanpkgstr(pkg_str):
    return pkg_str.replace('"', '').replace("'", '').strip()


def readresfile(res_file, rd_file):
    """ Read recommendation output file """

    dct_in = {}
    r_lines = []
    with open(rd_file) as rd_f:
        for line in rd_f:
            r_lines.append(cleanpkgstr(line.rstrip().replace(';', '')))

    with open(res_file) as res_f:
        for line in res_f:
            line_lst = line.rstrip().split(',', 1)
            r_pkg = cleanpkgstr(line_lst[0])
            if r_pkg in r_lines:
                try:
                    lst_apps = re.findall('''["']+([a-zA-Z._0-9]+)["']+''',
                                          line_lst[1])
                    dct_in[r_pkg] = lst_apps
                except IndexError:
                    dct_in[r_pkg] = []

    print('\t\t\tRecom and Top - check')

    return dct_in


def readinfile(in_file):
    """ Read input file """
    global lst_in

    if not lst_in:
        lst_in = []
        with open(in_file) as in_f:
            for line in in_f:
                lst_in.append(set(line.rstrip().split()))

    print('\t\t\tUsers file - nlines = {} -> check!'.format(len(lst_in)))
    return lst_in


def computedcg(set_pkg, lst_rec, num_recom):
    """ Compute DCG for a list of recommended apks against the User apks """
    if len(lst_rec) > num_recom:
        lst_rec = lst_rec[0:int(num_recom)]

    intersection = set_pkg.intersection(lst_rec)
    if not intersection:
        return 0.0

    dcg = 0
    for ele in intersection:
        dcg += (1 - lst_rec.index(ele) / num_recom)

    try:
        dcg /= float(len(set_pkg) - 1)  # normalized dcg divided by the number of user pkgs
    except ZeroDivisionError:
        return 0.0

    return dcg


def evalreco(dct_re, num_recom):
    """ Computes the sum of the DCG and the number of packages that have ..."""
    dct_dcg = {}
    apks_not_in_valid = set()
    for key in dct_re:
        aux_sum_dcg = 0.0
        aux_n = 0

        for line in lst_in:
            if key not in line:
                continue

            # if len(line) < num_recom + 1:
            #     continue

            aux_sum_dcg += computedcg(line, dct_re[key], num_recom)
            aux_n += 1

        if aux_n != 0:
            coef_normal = (num_recom + 1) / 2.0  # (num_recom * (num_recom + 1)) / (num_recom * 2.0)
            coef_normal = coef_normal / num_recom  # normalization
            dct_dcg[key] = (aux_sum_dcg / coef_normal) / float(aux_n)
        else:
            apks_not_in_valid.add(key)

    aux_sum = 0.0
    for ele in dct_dcg:
        aux_sum += dct_dcg[ele]

    return [aux_sum, len(dct_dcg)]


def pwsumlenavg(aux_sum, len_dct, dt):
    """ Print the result of the evaluation """
    print '\t\t\tSum of dcg for all apks: {}'.format(aux_sum)
    print '\t\t\tNumber of apks with dcg computed: {}'.format(len_dct)
    try:
        print '\t\t\tavg: {}'.format(aux_sum / len_dct)
    except ZeroDivisionError:
        print '\t\t\tCan not compute avg with number of apks equal to 0'
    print '\t\t\tDT: {} seconds.'.format(dt)


def write2file(filename, text):
    """ Write result file to working directory """
    with open(filename, 'a+') as f:
        f.write(text)


def write2s3(filename, text):
    """ Write result file to s3://aptoide-big-data/reco/results/FILENAME """
    if flag_b:
        s3 = boto3.resource('s3')
        filename = 'reco/results/' + filename
        s3.Object('aptoide-big-data', filename).put(Body=text)


def filename_text(aux_sum, len_dct, in_file, res_file, rd_file, dt):
    inp = getname(in_file)
    res = getname(res_file)
    rd = getname(rd_file)

    n = '{}{}{}'.format(datetime.datetime.now().hour,
                        datetime.datetime.now().minute,
                        datetime.datetime.now().second)

    filename = '{}_result_{}'.format(n, str(datetime.date.today()).replace('/', '').replace('-', ''))
    try:
        text = '{}, {}, {}, {}, {}, {}, {}\n'.format(inp, res, rd,
                                                     aux_sum, len_dct,
                                                     aux_sum / len_dct, dt)
    except ZeroDivisionError:
        text = '{}, {}, {}, {}, {}, {}, {}\n'.format(inp, res, rd,
                                                     aux_sum, len_dct,
                                                     0.0, dt)

    return [filename, text]


def getname(filed):
    """ Get name of the file "filed" without the extension and path """
    return os.path.splitext(os.path.basename(filed))[0]


def main(in_file, res_file, rd_file, num_recom=20):
    """ Main function of the evalrecom script """
    global lst_in
    num_recom = float(num_recom)

    st = datetime.datetime.now()
    lst_in = readinfile(in_file)
    dct_re = readresfile(res_file, rd_file)

    aux_sum, len_dct = evalreco(dct_re, num_recom)

    et = datetime.datetime.now()
    dt = (et - st).total_seconds()
    pwsumlenavg(aux_sum, len_dct, dt)
    filename, text = filename_text(aux_sum, len_dct, in_file,
                                   res_file, rd_file, dt)

    write2file(filename, text)
    # write2s3(filename, text)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Evaluation of the recommendation engine based on the '
                    'recommendation output file produced')
    parser.add_argument(
        'users',
        help='validation file')
    parser.add_argument(
        'recom',
        help='recommendation output file produced by the recommendation'
             ' engine')
    parser.add_argument(
        'rando',
        help='file with n random apk from top 100k that will be'
             ' tested')
    parser.add_argument(
        'num_recom',
        help='defines number of generated recommendations per'
             ' each queried item')

    args = parser.parse_args()

    main(args.users,
         args.recom,
         args.rando,
         args.num_recom)
