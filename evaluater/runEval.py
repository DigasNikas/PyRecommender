"""
Version: 0.4

Written by Pedro Santos

Change log:
        - num_recom is passed as input argument to evalrecom method (Ehsan.Nia 22-08-2016)

"""
import evalRecomColaborative

if __name__ == '__main__':
    path = '/opt/aptoide/evalrecom'
    num_recom = 40

    valid = ['valid_file']
    testapkids = ['testapkids_top50',
                  'testapkids_top1000',
                  'testapkids_top10k',
                  'testapkids_50_2016_08_05.txt',
                  'testapkids_1000_2016_08_05.txt',
                  'testapkids_10k_2016_08_05.txt']

    reco = ['DN_180916_lk_s1', ]

    for ele in valid:
        evalRecomColaborative.cleanlstin()
        print '{}:'.format(ele)
        for rec in reco:
            print '\t{}:'.format(rec)
            for t in testapkids:
                print '\t\t{}:'.format(t)
                evalRecomColaborative.main(path + '/valid_file/{}'.format(ele),
                                           path + '/reco_files/{}'.format(rec),
                                           path + '/testapks/{}'.format(t), num_recom)
