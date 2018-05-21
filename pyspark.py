from pyspark import SparkContext
import json
sc=SparkContext.getOrCreate()

#Baca data dari storage hdfs
rdd = sc.textFile("hdfs://192.168.56.121:8020/dummy.json")

#Cek satu hari, ex: 17 agustus
def cekTgl(tgl):
	def _cekTgl(data):
		dataJson=json.loads(data)
		temp=[]
		tglSplit=tgl.split(" ")
		for i in dataJson["id"]:
			waktuSplit=dataJson["id"][i]["waktu"].split(" ")
			if waktuSplit[0] == tglSplit[0] and waktuSplit[1] == tglSplit[1]:
				temp.append(dataJson["id"][i])
		return temp
	return _cekTgl


#Cek rentang waktu
#2 parameter, ex: cek 17 mei - 20 mei
#3 parameter, ex: status 'ancaman' pada 17 mei - 20 mei
def cekRentang(waktux,waktuy,status=None):
        def _cekRentang(data):
                dataJson=json.loads(data)
                temp=[]
                waktuxSplit=waktux.split(" ")
                waktuySplit=waktuy.split(" ")
                for i in dataJson["id"]:
                        dataSplit=dataJson["id"][i]["waktu"].split(" ")
                        if dataSplit[0] >= waktuxSplit[0] and dataSplit[0] <= waktuySplit[0] and dataSplit[1] == waktuxSplit[1]:
				if status is not None:
	                                if dataJson["id"][i]["status"]==status:
						temp.append(dataJson["id"][i])
				else:
					temp.append(dataJson["id"][i])
                return temp
        return _cekRentang

#rddA=rdd.map(cekTgl("15 mei"))
#rddA=rdd.map(cekRentang("16 mei", "17 mei"))
rddA=rdd.map(cekRentang("15 mei","17 mei","ancaman"))

#simpan dalam file
rddA.saveAsTextFile("hdfs://192.168.56.121:8020/hasil_dummy")
