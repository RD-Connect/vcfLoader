import sys
import os
for arg in sys.argv:
    print arg
UMDurl="139.124.156.147/webservices/webservices_put.php?token=MyRDConnectToken"
prefix ="/Users/dpiscia/RD-repositories/GenPipe/out/V5.1/"
for i in range(int(sys.argv[1]),int(sys.argv[2])):
      path=prefix+"umd/chrom"+str(i)
      command1="cp "+path+"/part-00000 "+path+"/chrom"+str(i)+".vcf"
      print(command1)
      os.system(command1)
      command2= "curl -X PUT -F file=@"+path+"/chrom"+str(i)+".vcf " +UMDurl+ " > "+prefix+"/umd/chrom"+str(i)+".annotated"
      print(command2)
      os.system(command2)

#curl -X PUT -F file=@./chrom1.vcf  UMDurl > chrom1.annotated
