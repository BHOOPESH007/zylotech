from script_file import run_script
from pyspark_file import PysparkReader


if __name__ == "__main__":
    print '---------------------script is running--------------------'
    #TODO: comment once script ran
    run_script()
    print "file1.csv and file2.csv has been created"
    print '------------------------Done-----------------------'

    while 1:
        category=input('category : ')
        if not isinstance(category, list):
            print 'category should be list \nPlease enter '
            continue
        type=raw_input('Please Enter 1 for age average or 2 for height average : ')
        
        if type=='1':
            type='age'
        elif type=='2':
            type='height'
        else:
            print 'Oops! invalid input'
            break

        print 'Average_'+ type + ' is : ', PysparkReader().get_avg(category,type)
        
        is_continue=raw_input('Do you want to continue?[y/n] ')
        if is_continue in ['y', 'Y']:
            continue
        break

