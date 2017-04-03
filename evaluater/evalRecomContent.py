"""
evalRecomWithFile.py

Written by Pedro Santos

Change log:
        -(Diogo Nicolau: Shanon Index)
        -(Diogo Nicolau: Apps in google unique)
        -(Diogo Nicolau: New Metric A)
        -(Diogo Nicolau: New Metric B)
        -(Ehsan.Nia 17-09-2016)
        
Parameters:
		- vfil: validaiton file from google similar apps. format: QueryApp,app1; app2; ...; appn	
		- ifil: input recommendation file. format: "QueryApp",("app1",x1),...,("appn",xn)
		- n   :  number of recommendations per QueryApp to consider (in the ifil file).
sample usage:
		python evalRecomWithFile.py googleplay_app_sim.txt  dsw34_results.txt 40
"""
import re
import argparse
import os
import datetime
from auxFuncs import uniqueNum, incAppCount
from math import log1p

def cleanPkgStr(pkg_str):
    return pkg_str.replace('"', '').replace("'", '').strip()


def readResFile(res_file):
    """ Read recommendation output file """
    global total_apps_in_Res
    global unq_apps_in_Res_counter
    global unq_apps_in_Res
    dct_in = {}
    cntr_apps_with_noRecom = 0
    j = 0

    with open(res_file) as res_f:
        for line in res_f:
            line_lst = line.rstrip().split(',', 1)
            r_pkg = cleanPkgStr(line_lst[0])
            try:
                lst_apps = re.findall('''["']+([a-zA-Z._0-9]+)["']+''',
                                      line_lst[1])
                dct_in[r_pkg] = lst_apps
                j+=1
                """uniqueNum(unq_apps_in_Res, r_pkg)"""
                total_apps_in_Res += 1
                for i in lst_apps:
                    uniqueNum(unq_apps_in_Res, i)
                    incAppCount(unq_apps_in_Res_counter, i)
                    total_apps_in_Res += 1
            except IndexError:
                dct_in[r_pkg] = []
                j+=1
                cntr_apps_with_noRecom += 1
            if j != len(dct_in):
                print("Duplicate apks in the result file, line: {}, apk: {}!".format(j, r_pkg))
                j -= 1

    print('\tRecom - check')
    print("\n\tNumber of packages in result file with no recommendation provided: {}".format(cntr_apps_with_noRecom))
    return dct_in

def readValFile(in_file):
    """ Read validation input file """
    global total_apps_in_Val
    lst_in = {}
    with open(in_file) as in_f:
        for line in in_f:
            line = line.strip().split(',')
            pkg = line[0]
            uniqueNum(unq_apps_in_Val, pkg)
            total_apps_in_Val += 1
            sim_pkgs = line[1].split(';')
            lst_in[pkg] = set(sim_pkgs)
            for i in sim_pkgs:
                uniqueNum(unq_apps_in_Val, i)
                total_apps_in_Val += 1

    print('\tValidation file - check')
    return lst_in

def computeShannon():
    Final = {}
    for i in unq_apps_in_Res_counter:
        try:
            proportion = float(unq_apps_in_Res_counter[i]) / float(total_apps_in_Res)
        except ZeroDivisionError:
            return 0
        logCalcul = log1p(proportion)
        Final[i] = proportion * logCalcul
    shannon = 0
    for i in Final:
        shannon += Final[i]

    expectedShannon = log1p(total_apps_in_Res)

    return shannon, expectedShannon



def computeDCG(lst_pkg, lst_rec, numRecom, uniqueApps):
    """ Compute DCG for a list of recommended apks against the User apks """
    if len(lst_rec) > numRecom:
        lst_rec = lst_rec[0:int(numRecom)]

    intersection = lst_pkg.intersection(lst_rec)
    if not intersection:
        return 0, 0

    dcg = 0.0
    for ele in intersection:
        dcg += (1 - lst_rec.index(ele)/numRecom)
        uniqueNum(uniqueApps, ele)

    try:
        dcg = dcg / float(len(lst_pkg)) # normalized dcg divided by the number of user pkgs
    except ZeroDivisionError:
        return 0, 0

    return dcg, len(intersection)


def evalReco(lst_val, dct_res, numRecom):
    """ Computes the sum of the DCG and the number of packages that have ..."""
    dct_dcg = {}
    apks_not_in_valid = set()
    cntr_apks_in_valid = 0
    cntr_apks_in_valid_unique = 0
    uniqueApps = {}

    for key in dct_res:
        aux_sum_dcg = 0.0
        aux_n = 0

        if key in lst_val:
            aux_sum_dcg_tmp1, num_of_intersection_tmp2 = computeDCG(lst_val[key], dct_res[key], numRecom, uniqueApps)
            aux_sum_dcg += aux_sum_dcg_tmp1
            k = cntr_apks_in_valid
            cntr_apks_in_valid += num_of_intersection_tmp2
            if k != cntr_apks_in_valid:
                cntr_apks_in_valid_unique += 1
            # normalization: coef_normal = coef_normal / numRecom
            coef_normal = (numRecom + 1) / (2.0)     # (numRecom * (numRecom + 1)) / (numRecom * 2.0)
            dct_dcg[key] = (aux_sum_dcg / coef_normal) / float(len(lst_val[key]))
        else:
            apks_not_in_valid.add(key)

    aux_sum = 0.0
    for ele in dct_dcg:
        aux_sum += dct_dcg[ele]

    print '\tNumber of apks in result file with dcg NOT computed (apk not found in validation file): {}'.format(len(apks_not_in_valid))
    print '\tNumber of apks in result file with dcg computed: {}'.format(len(dct_dcg))
    print '\tTotal number of apks from recommendation in results file which are also in the recommendation in validation file: {}'.format(cntr_apks_in_valid)

#    for ele in apks_not_in_valid:
#        print("\t"+ele)

    return [aux_sum, len(dct_dcg), cntr_apks_in_valid, len(uniqueApps)]


def pwSumLenAvg(aux_sum, len_dct, dt):

    dcg = aux_sum / len_dct

    """ Print the result of the evaluation """
    print '\n\tSum of dcg for all apks: {}'.format(aux_sum)
    print '\tNumber of apks with dcg computed: {}'.format(len_dct)
    try:
        print '\tavg: {}'.format(aux_sum / len_dct)
    except ZeroDivisionError:
        print '\tCan not compute avg with number of apks equal to 0'
    print '\tDT: {} seconds.'.format(dt)
    print '\n'

    return [dcg,len_dct]


def filename_text(aux_sum, len_dct, in_file, res_file, dt):
    inp = getName(in_file)
    res = getName(res_file)

    n = '{}{}{}'.format(datetime.datetime.now().hour, 
                        datetime.datetime.now().minute, 
                        datetime.datetime.now().second)

    filename = '{}_result_{}'.format(n, 
                                     str(
                                         datetime.date.today())\
                                                       .replace('/','')\
                                                       .replace('-',''))
    try:
        text = '{}, {}, {}, {}, {}, {}\n'.format(inp, res,
                                                     aux_sum, len_dct, 
                                                     aux_sum / len_dct, dt)
    except ZeroDivisionError:
        text = '{}, {}, {}, {}, {}, {}\n'.format(inp, res,
                                                     aux_sum, len_dct, 
                                                     0.0, dt)

    return [filename, text]


def getName(filed):
    """ Get name of the file "filed" without the extension and path """
    return os.path.splitext(os.path.basename(filed))[0]


def main(val_file, res_file, numRecom):
    """ Main function of the evalrecom script """
    numRecom = float(numRecom)

    res_files = os.listdir(res_file)
    print("\tFiles to work: ")
    print(res_files)
    lst_val = readValFile(val_file)
    print("\n\tNumber of lines in validation file: {}".format(len(lst_val)))
    print("\tNumber of unique apps in validation file: {}".format(len(unq_apps_in_Val)))
    print("\tPercentage of unique apps in validation file: {} %".format(
        int((len(unq_apps_in_Val) / float(total_apps_in_Val)) * 100)))
    logfile.write("Number of lines in validation file: {}\n".format(len(lst_val)))
    logfile.write("Number of unique apps in validation file: {}\n".format(len(unq_apps_in_Val)))
    logfile.write("Percentage of unique apps in validation file: {}%\n".format(
        int((len(unq_apps_in_Val) / float(total_apps_in_Val)) * 100)))
    logfile.write("\n")
    for res in res_files:
        st = datetime.datetime.now()
        dct_res = readResFile(res_file+res)
        shannon, expectedShannon = computeShannon()

        print("\tWorking result file: {}".format(res))
        print("\tTotal Number of Recommendations: {} ".format(total_apps_in_Res))
        print("\tNumber of lines in result file: {}".format(len(dct_res)))
        print("\tNumber of unique apps in results file: {}".format(len(unq_apps_in_Res)))
        print("\tPercentage of unique apps in result file: {}%".format(int((len(unq_apps_in_Res)/float(total_apps_in_Res))*100)))

        logfile.write("Working result file: {}\n".format(res))
        logfile.write("Total Number of Recommendations: {} \n".format(total_apps_in_Res))
        logfile.write("Number of lines in result file: {}\n".format(len(dct_res)))
        logfile.write("Number of unique apps in results file: {}\n".format(len(unq_apps_in_Res)))
        logfile.write("Percentage of unique apps in result file: {}%\n".format(int((len(unq_apps_in_Res)/float(total_apps_in_Res))*100)))

        aux_sum, len_dct, cntr_apks_in_valid, cntr_apks_unique = evalReco(lst_val, dct_res, numRecom)
        logfile.write("Total number of apks from recommendation in results file which are also in the recommendation in validation file: {}\n".format(cntr_apks_in_valid))

        et = datetime.datetime.now()
        dt = (et-st).total_seconds()

        dcg, len_dct = pwSumLenAvg(aux_sum, len_dct, dt)
        logfile.write("avg dcg: {}\n".format(dcg))
        variation = len(unq_apps_in_Res)/float(total_apps_in_Res)
        x1 = 10000
        x2 = 1
        new_metric = (x1 * dcg) + (x2 * variation)
        metric1_1 = float(total_apps_in_Res) / float((len(dct_res))*numRecom)
        metric1 = float(total_apps_in_Res) / float(100000*numRecom)
        metric2 = float(cntr_apks_in_valid) / float(len_dct)
        metric = metric1_1 * metric2

        print("\tNew Metric Value (dcg + variation): {} ".format(new_metric))
        logfile.write("New Metric Value (dcg + variation): {} \n".format(new_metric))
        print("\tNew Metric Value A (occurrences in google / tested apps in google): {} ".format(metric2))
        logfile.write("New Metric Value A (occurrences in google / tested apps in google): {} \n".format(metric2))
        print("\tNew Metric Value B (total recs / num_lines * 40): {} ".format(metric1_1))
        logfile.write("New Metric Value B (total recs / num_lines * 40): {} \n".format(metric1_1))
        print("\tNew Metric Value A * B: {} ".format(metric))
        logfile.write("New Metric Value A * B: {} \n".format(metric))
        print("\tApps in google unique: {}".format(cntr_apks_unique))
        logfile.write("Apps in google unique: {} \n".format(cntr_apks_unique))
        print("\tShannon Diversity Index: {}".format(shannon))
        logfile.write("Shannon Diversity Index: {} \n".format(shannon))
        print("\tMax Shannon Diversity Index: {}".format(expectedShannon))
        logfile.write("Max Shannon Diversity Index: {} \n".format(expectedShannon))
        print("\tShannon Equability: {}".format(shannon / float(expectedShannon)))
        logfile.write("Shannon Equability: {} \n".format(shannon / float(expectedShannon)))
        print("\n")

        spreadsheet.write("{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{}\n".format(res, total_apps_in_Res, len(dct_res), len(unq_apps_in_Res),
                                                                            cntr_apks_in_valid, cntr_apks_unique, float(cntr_apks_unique) / float(cntr_apks_in_valid),
                                                                            len_dct, dcg,
                                                                            new_metric, metric2, metric1_1, metric,
                                                                            shannon, expectedShannon, shannon / float(expectedShannon)))

        filename, text = filename_text(aux_sum, len_dct, val_file,
                                       res_file, dt)

        logfile.write(text)
        logfile.write("\n")
        global unq_apps_in_Res_counter
        unq_apps_in_Res_counter = {}
        global unq_apps_in_Res
        unq_apps_in_Res = {}
        global total_apps_in_Res
        total_apps_in_Res = 0
        global unq_apps_in_Val
        unq_apps_in_Val = {}
        global total_apps_in_Val
        total_apps_in_Val = 0


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Evaluation of the recommendation engine based on the '
        'recommendation output file produced')
    parser.add_argument(
        'val',
        help='validation file')
    parser.add_argument(
        'recom',
        help='recommendation output file produced by the recommendation'
        ' engine')
    parser.add_argument(
        'numRecom',
        help='defines number of generated recommendations per'
        ' each queried item')

    args = parser.parse_args()

    logfile = open("log.txt", 'w')
    spreadsheet = open('results.csv', 'w')
    spreadsheet.write("name,Total Recs,number lines,unique apps in res,total occurrences,unique occurrences,percentage of unique occurrences,"
                      "apps used for dcg,dcg,dcg+variation,occurrencesNorm,totalRecsNorm,occurrencesNorm * totalRecsNorm, shannon diversity index,"
                      "max shannon diversity index, shannon equability\n")
    logfile.write("\n")
    global unq_apps_in_Res_counter
    unq_apps_in_Res_counter = {}
    global unq_apps_in_Res
    unq_apps_in_Res = {}
    global total_apps_in_Res
    total_apps_in_Res = 0
    global unq_apps_in_Val
    unq_apps_in_Val = {}
    global total_apps_in_Val
    total_apps_in_Val = 0

    main(args.val,
         args.recom,
         args.numRecom)
    logfile.close()
    spreadsheet.close()
