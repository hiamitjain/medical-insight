from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, FloatType
from pyspark.sql.window import Window
from pyspark.sql.functions import split, sum, round, max, count, rank, desc, dense_rank
import sys
import time

'''
*** HealthCare Realated Information ****
*******************************************************************************************************************
This module provide information about Providers, their services, and the incurred cost for a given treatment.
It can also provide the cheapest and the costliest provider, and various insights on Inpatient data.
*******************************************************************************************************************
Here are the KEYWORDS to get the desired information:

--PROVIDER zip/city/state value
    Return Providers in a given ZIP/CITY/STATE. If provider not found for the ZIP then return information for 10 nearby 
    ZIP.
    
    ** USAGE **
    --PROVIDER ZIP 12345 
    --PROVIDER CITY 'NEW YORK'
    --PROVIDER STATE 'NY' 

-- DIAG zip/city/state value diagName 
    Return Providers in a given ZIP/CITY/STATE which treats the given medical condition. 
    
    ** USAGE **
    --DIAG ZIP 12345 'HEART'
    --DIAG CITY 'MADISON' 'KIDNEY'
    --DIAG STATE 'CA' 'BLOOD'
          
-- TOP H/L zip/city/state value
    H - Return TOP 10 providers with highest treatment cost in a given ZIP/CITY/STATE
    L - Return TOP 10 providers with cheapest treatment cost in a given ZIP/CITY/STATE
    
    ** USAGE **
    --TOP H zip 12345
    --TOP L city 'MADISON'
    
-- TOP-PAT H/L zip/city/state value
    H - Return Top 10 providers with highest Patient incurred cost in a given ZIP/CITY/STATE
    L - Return Top 10 providers with cheapest Patient incurred cost in a given ZIP/CITY/STATE
    
    ** USAGE **
    --TAP-PAT H zip 12345
    --TAP-PAT L city 'MADISON'
    
-- TOP-MED H/L zip/city/state value
    H - Return Top 10 providers with highest Total Medical cost in a given ZIP/CITY/STATE
    L - Return Top 10 providers with cheapest Total Medical cost in a given ZIP/CITY/STATE  
    
    ** USAGE **
    --TAP-MED H zip 12345
    --TAP-MED L city 'MADISON'    
    
-- TOP-DIA-PAT H/L zip/city/state value
    H - Return Top 10 Treatments with highest Patient cost in a given ZIP/CITY/STATE
    L - Return Top 10 Treatments with cheapest Patient cost in a given ZIP/CITY/STATE
    
    ** USAGE **
    --TAP-DIA-PAT H zip 12345
    --TAP-DIA-PAT L city 'MADISON'    
    
-- TOP-DIA-MED H/L zip/city/state value
    H - Return Top 10 Treatments with highest Total Medical cost in a given ZIP/CITY/STATE
    L - Return Top 10 Treatments with cheapest Total Medical cost in a given ZIP/CITY/STATE    
    
    ** USAGE **
    --TAP-DIA-MED H zip 12345
    --TAP-DIA-MED L city 'MADISON'
    
-- TOP-DIA H/L 
    H - Return Top 10 Treatments with highest Total Medical cost in Country
    L - Return Top 10 Treatments with cheapest Total Medical cost in Country    
    
    ** USAGE **
    --TAP-DIA H zip 12345
    --TAP-DIA L city 'MADISON' 
'''


start_time = time.time()

spark = SparkSession.builder.appName('HospitalProject').master('local[2]').getOrCreate()
spark.conf.set("spark.sql.shuffle.partitions", '2')

def getPaymentData(filename):
    fRead = spark.read.csv(filename, header=True)
    paymentData = fRead.select(fRead.DRGDefinition, fRead.ProviderId.cast(FloatType()),
                               fRead.TotalDischarges.cast(FloatType()),
                               fRead.AverageTotalPayments.cast(FloatType()),
                               fRead.AverageMedicarePayments.cast(FloatType()),
                                (fRead.AverageTotalPayments - fRead.AverageMedicarePayments).
                                        cast(FloatType()).alias('AvgPatientPayments'))
#                            withColumn("DRGNo", substring("DRGDefinition", 1, 3).cast(IntegerType())). \
#                            drop("DRGDefinition")
    return paymentData

def getDIAGData(fRead, data):
    DRGData = fRead.select('DRGDefinition').dropDuplicates()
    func = split(DRGData.DRGDefinition, '-')
    DRGData = DRGData.withColumn('DRGNo', func.getItem(0)). \
                withColumn('DRGName', func.getItem(1)). \
                drop('DRGDefinition').orderBy('DRGNo')
    a = '%' + data + '%'
    return DRGData.filter(DRGData.DRGName.like(a))

def getProviderData(inputdata):
    providerData = inputdata.select(inputdata.ProviderId.cast(IntegerType()), inputdata.ProviderName,
                                    inputdata.ProviderStreetAddress,inputdata.ProviderCity,
                                    inputdata.ProviderState,inputdata.ProviderZipCode.cast(IntegerType())). \
                            drop_duplicates()
    return providerData

def getProviderCost(inputdata):

    wSpec1 = Window.partitionBy(inputdata.ProviderId)
    diff = inputdata.AverageTotalPayments.cast(FloatType()) - inputdata.AverageMedicarePayments.cast(FloatType())
    tempData = inputdata.select(inputdata.ProviderId.cast(IntegerType()), inputdata.ProviderName,
                                inputdata.ProviderStreetAddress,inputdata.ProviderCity,
                                inputdata.ProviderState,inputdata.ProviderZipCode.cast(IntegerType()),
                                round((sum(diff).over(wSpec1)/count(diff).over(wSpec1)), 2).cast(FloatType()).
                                    alias('PatientExp'),
                                (sum(inputdata.AverageTotalPayments).over(wSpec1)/count(inputdata.AverageTotalPayments).
                                    over(wSpec1)).cast(FloatType()).alias('prvtot')). \
                         orderBy('prvtot', ascending = False). \
                         drop_duplicates()
    wSpec = Window.partitionBy(tempData.ProviderState)
    providerData = tempData.withColumn('TotalCostPercentile', round(tempData.prvtot*100/max(tempData.prvtot).
                                                                    over(wSpec), 2)). \
                            withColumn('PatientExpPercentile', round(tempData.PatientExp * 100/max(tempData.PatientExp).
                                                                     over(wSpec), 2)). \
                            drop('prvtot').orderBy('PatientExpPercentile', ascending=False)

    return providerData

def getTotalExpState(filename):
    fRead = spark.read.csv(filename, header=True)
    wSpec = Window.partitionBy(fRead.ProviderState)
    data = fRead.select(fRead.ProviderState,
                        (sum(fRead.AverageTotalPayments).over(wSpec)/count(fRead.AverageTotalPayments).over(wSpec)).
                        cast(FloatType()).alias('StateTotal')). \
                        orderBy('StateTotal', ascending=False). \
                        drop_duplicates()

    wSpec1 = Window.partitionBy()
    totaExp = data.withColumn('StateExpPercentile', round(data.StateTotal*100/max(data.StateTotal).over(wSpec1), 2))
    return totaExp

def getTopExpCity(filename, type):
    fRead = spark.read.csv(filename, header=True)

    if type == 'total':
        fReadAdd = fRead.select(fRead.ProviderState,fRead.ProviderCity,fRead.TotalDischarges,
                         (fRead.AverageTotalPayments*fRead.TotalDischarges).cast(FloatType()).alias('TotalPayments'))
    elif type == 'medicare':
        fReadAdd = fRead.select(fRead.ProviderState, fRead.ProviderCity, fRead.TotalDischarges,
                         (fRead.AverageMedicarePayments*fRead.TotalDischarges).cast(FloatType()).alias('TotalPayments'))
    elif type == 'patient':
        amtDiff = fRead.AverageTotalPayments - fRead.AverageMedicarePayments
        fReadAdd = fRead.select(fRead.ProviderState, fRead.ProviderCity, fRead.TotalDischarges,
                            (amtDiff * fRead.TotalDischarges).cast(FloatType()).alias('TotalPayments'))
    else:
        print('Invalid Request Type for total payments. Valid Request are total/medicare/patient')
        return

    wSpec1 = Window.partitionBy(fReadAdd.ProviderState)
    wSpec2 = Window.partitionBy(fReadAdd.ProviderState, fReadAdd.ProviderCity)
    data = fReadAdd.withColumn('CityTotal', round((sum('TotalPayments').over(wSpec2)/sum('TotalDischarges').over(wSpec2)), 2)). \
                    withColumn('StateTotal', round(sum('TotalPayments').over(wSpec1)/sum('TotalDischarges').over(wSpec1), 2)). \
                    drop('TotalDischarges').drop('TotalPayments').drop_duplicates()

    data1 = data.withColumn('rank', dense_rank().over(Window.partitionBy('ProviderState').orderBy(desc('CityTotal'))))

    wSpecA = Window.partitionBy().orderBy(data.StateTotal.desc())
    wSpecB = Window.partitionBy(data.ProviderState).orderBy(data.CityTotal.desc())
    totaExp = data.withColumn('StateExpPercentile', round(data.StateTotal*100/max(data.StateTotal).over(wSpecA), 2)). \
                    withColumn('Staternk', dense_rank().over(wSpecA)). \
                    withColumn('CityPercentile', round(data.CityTotal*100/max(data.CityTotal).over(wSpecB), 2)). \
                    withColumn('Cityrnk', dense_rank().over(wSpecB)). \
                    drop('StateTotal').drop('CityTotal')

    return totaExp.filter(totaExp.Cityrnk < 3).orderBy(totaExp.Staternk, totaExp.Cityrnk)
#    return data1.filter(data1.rank < 3)

def diagAnalysis(filename):
    inputdata = spark.read.csv(filename, header=True)
    wSpec = Window.partitionBy('DRGDefinition')
    data = inputdata.select('DRGDefinition', inputdata.AverageTotalPayments.cast(FloatType())). \
                     withColumn('max', max('AverageTotalPayments').over(wSpec)). \
                     drop('AverageTotalPayments').drop_duplicates(). \
                     withColumn('rank', dense_rank().over(Window.partitionBy().orderBy('max')))
    return data.filter(data.rank < 21)


def getTreatment(inputdata, diag):
    data = inputdata.select('DRGDefinition', 'ProviderName', 'ProviderStreetAddress', 'ProviderCity', 'ProviderState',
                            'ProviderZipCode')
    value = '%' + diag + '%'
    return data.filter(data.DRGDefinition.like(value))


def filterByX(filename, x, value):
    if x == 'zip':
        inputdata = spark.read.csv(filename, header=True)
        data = inputdata.filter(inputdata.ProviderZipCode == int(value))
        if not data.first():
            data = inputdata.filter(inputdata.ProviderZipCode.between(int(value) - 5, int(value) + 5))
    else:
        if x == 'city':
            inputdata = spark.read.csv(filename, header=True)
            data = inputdata.filter(inputdata.ProviderCity == value.upper())
        else:
            if x == 'state':
                inputdata = spark.read.csv(filename, header=True)
                data = inputdata.filter(inputdata.ProviderState == value.upper())
            else:
                print('%s value is invalid. It should be zip, city, state' %x)
                end_time = time.time()
                print('This transaction took %s' %(end_time - start_time))
                sys.exit(1)
    if data.first():
        return data
    else:
        print('No data for %s %s' %(value, x))
        end_time = time.time()
        print('This transaction took %s' % (end_time - start_time))
        sys.exit(1)


def main():
    filename = 'C:/user/Medicare/InpatientCharges_FY2011.csv'

    if sys.argv[1] == '--PROVIDER':
        getTreatment(filterByX(filename, sys.argv[2], sys.argv[3])).show()
    elif sys.argv[1] == '--DIAG':
        getTreatment(filterByX(filename, sys.argv[2], sys.argv[3]), sys.argv[4]).show()
    else:
        print('Invalid option. Valid options are:')

#    data = filterByX(filename, 'state', 'NY')
#    getProviderData(data).show()
#    getProviderCost(data).show()
#    getTotalExpState(filename).show()
#    getTopExpCity(filename, 'patient1').show()
#    diagAnalysis(filename).show()
#    getTreatment(filterByX(filename, 'state', 'WI')).show()
#    getDIAGData(filename).collect()
#    getPaymentData(filename).show()



    end_time = time.time()
    print('This transaction took %s' % (end_time - start_time))

if __name__ == '__main__':
    main()

else:
    print('Program should not be called from another program')
