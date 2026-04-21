
#R code to read in CanCHEC eSCHIF parameters and Fusion paramters for non-accidental causes of death, create Fusion-CanCHEC and GEMM RR estimates, and excess deaths for each country based on its population-weighted PM2.5 concentration
#Code adapted from Weichenthal et al. (2022)

library(MASS)
library(readxl)
#Create Fuson RR estimates over PM2.5 range, here it is from 0 to 120 ug/m3
# If PM2.5 exceeds 120 ug/m3, replace 120 by the maximum PM2.5 round off to nearest integer

#Set working directory where the code and relevant datasets have been stored
setwd("~/Health_burden_directory")
FUSparm<- read.csv("Fusion Non-Accidental Deaths Parameters.csv", header=T)
gr=FUSparm[,1]
C=FUSparm[,2]
p=FUSparm[,3]
T=74
nsim=1000
lamda=(T-C)/(T*(1-p))
xx=seq(0, T, 0.1)
G=matrix(0, nsim, length(xx)) 
for (j in 1:nsim){
  G[j,]=1/(1+((1-p[j])/p[j])*(ifelse(xx<C[j], 0, xx-C[j])/(T-C[j]))^lamda[j])}
INT=matrix(0, nsim, length(xx))
for (j in 1:nsim) {
  for (k in 1:length(xx)){
    INT[j,k]=sum(G[j,1:k]*0.1)
  }}

x=seq(0, 120, 0.1)
#FUSION is matrix of 1000 sets of RR estimates over 0 to 120ug/m3 by 0.1ug/m3
FUSION=matrix(0, nsim, length(x))
for (j in 1:nsim) {
  for (k in 1:length(x)) {
    if (x[k]<T) {FUSION[j,k]=gr[j]*INT[j,k]}
    if (x[k]>=T) {FUSION[j,k]=gr[j]*(INT[j,length(xx)]+T*log(max(x[k],T)/T)*p[j])}
  }}

#eSCHIF RR estimates based on CanCHEC cohort
NACparm<- read.csv("eSCHIF CanCHEC Non-Accidental Deaths Parameters.csv", header=T)
int=NACparm[,1]
gamma=NACparm[,2]
delta=NACparm[,3]
theta=NACparm[,4]
alpha=NACparm[,5]
mu=NACparm[,6]
v=NACparm[,7]
nsim=1000
z=seq(0, 10-2.5, 0.1)  
eSCHIFNAC=matrix(0, nsim, length(z))
for (j in 1:nsim) {
  eSCHIFNAC[j,]=int[j] + gamma[j]*log(z/delta[j]+1) + theta[j]*log(z/alpha[j]+1)/(1+exp(-(z-mu[j])/(v[j]))) 
}
meanriskNAC=matrix(0, length(z), 1)
uclNAC=matrix(0, length(z), 1)
lclNAC=matrix(0, length(z), 1)
for (j in 1:length(z)) {
  meanriskNAC[j]=exp(mean(eSCHIFNAC[,j]))
  uclNAC[j]=exp(quantile(eSCHIFNAC[,j], 0.975))
  lclNAC[j]=exp(quantile(eSCHIFNAC[,j], 0.025))
}

#Construct hybrid RR model of eSCHIF CanCHEC RR <9.8ug/m3 and Fusion >=9.8ug/m3
# PM2.5 threshold for transition is 9.8 ug/m3 (Marais et al. 2023) and not 5 ug/m3 as in Weichenthal et al. (2022)
CHEC=matrix(0, nsim, 101) 
for (j in 1:nsim) {
  CHEC[j,]=c(rep(0, 25), eSCHIFNAC[j,])}

HYB=matrix(0, nsim, length(x))
for (j in 1:nsim) {
  for (k in 1:length(x)) {
    ind=10*x[k]+1
    if (x[k]<9.8) {HYB[j,k]=CHEC[j,k] }
    if (x[k]>=9.8) {HYB[j,k]=  CHEC[j,99] + (FUSION[j,ind]-FUSION[j,99]) }
  }}

#GEMM from Burnett et al. (2018)
#GEMM1 includes the Chinese male cohort, relevant for high PM2.5 concentrations in China/India
#GEMM2 excludes the Chinese male cohort, relevant for low PM2.5 concentrations in UK/US
GEMM1=matrix(0, nsim, length(x))
GEMM1_low=matrix(0, nsim, length(x))
GEMM1_high=matrix(0, nsim, length(x))
for (j in 1:nsim) {
  for (k in 1:length(x)) {
    ind=10*x[k]+1
    GEMM1[j,k]=log(1+(max(0,x[k]-2.4)/1.6))*0.143/(1+exp(-(max(0,x[k]-2.4)-15.5)/36.8)) 
    GEMM1_low[j,k]=log(1+(max(0,x[k]-2.4)/1.6))*(0.143-1.96*0.01807)/(1+exp(-(max(0,x[k]-2.4)-15.5)/36.8)) 
    GEMM1_high[j,k]=log(1+(max(0,x[k]-2.4)/1.6))*(0.143+1.96*0.01807)/(1+exp(-(max(0,x[k]-2.4)-15.5)/36.8)) 
  }}

GEMM2=matrix(0, nsim, length(x))
GEMM2_low=matrix(0, nsim, length(x))
GEMM2_high=matrix(0, nsim, length(x))
for (j in 1:nsim) {
  for (k in 1:length(x)) {
    ind=10*x[k]+1
    GEMM2[j,k]=log(1+(max(0,x[k]-2.4)/1.5))*0.1231/(1+exp(-(max(0,x[k]-2.4)-10.4)/25.9))
    GEMM2_low[j,k]=log(1+(max(0,x[k]-2.4)/1.5))*(0.1231-1.96*0.01797)/(1+exp(-(max(0,x[k]-2.4)-10.4)/25.9))
    GEMM2_high[j,k]=log(1+(max(0,x[k]-2.4)/1.5))*(0.1231+1.96*0.01797)/(1+exp(-(max(0,x[k]-2.4)-10.4)/25.9)) 
  }}

#read in number of non-accidental deaths in 2019 by 5-year age group and by country and population weighted PM2.5 concentration
# If you have grid cell level dataset, have them in the same format (data columns in the same order)
NACparm<- as.matrix(read.csv("Deaths by Age and Country with No Country Labels.csv", header=T))
dim2=dim(NACparm)[2]
dim1=dim(NACparm)[1]
NACdeaths=matrix(0, dim1, 1)
for (j in 1:dim1) {NACdeaths[j]=sum(NACparm[j,3:17])} #change if data in different columns
PM=NACparm[,2]
pop=NACparm[,1]

#Calculate population attributable fraction based on  both Fusion-CanCHEC and GEMM RR model: PAF=1-1/RR
IND=10*PM+1

HYBRR=matrix(0, nsim, dim1)
GEMM1RR=matrix(0, nsim, dim1)
GEMM2RR=matrix(0, nsim, dim1)
GEMM1_lowRR=matrix(0, nsim, dim1)
GEMM2_lowRR=matrix(0, nsim, dim1)
GEMM1_highRR=matrix(0, nsim, dim1)
GEMM2_highRR=matrix(0, nsim, dim1)

for (j in 1:nsim) {
  print(j)
  for (k in 1:dim1) {

  HYBRR[j,k]=HYB[j, IND[k]]-HYB[j, 1]
  GEMM1RR[j,k]=GEMM1[j, IND[k]]-GEMM1[j, 1]
  GEMM1_lowRR[j,k]=GEMM1_low[j, IND[k]]-GEMM1_low[j, 1]
  GEMM1_highRR[j,k]=GEMM1_high[j, IND[k]]-GEMM1_high[j, 1]
  GEMM2RR[j,k]=GEMM2[j, IND[k]]-GEMM2[j, 1]
  GEMM2_lowRR[j,k]=GEMM2_low[j, IND[k]]-GEMM2_low[j, 1]
  GEMM2_highRR[j,k]=GEMM2_high[j, IND[k]]-GEMM2_high[j, 1]
  }}

HYBPAF=matrix(0, nsim, length(PM))
GEMM1PAF=matrix(0, nsim, length(PM))
GEMM2PAF= matrix(0, nsim, length(PM))
GEMM1_lowPAF=matrix(0, nsim, length(PM))
GEMM2_lowPAF= matrix(0, nsim, length(PM))
GEMM1_highPAF=matrix(0, nsim, length(PM))
GEMM2_highPAF= matrix(0, nsim, length(PM))

for (j in 1:nsim) {
  print(j)
  for (k in 1:length(PM)){

    HYBPAF[j,k]=1-1/exp(HYBRR[j,k])
    GEMM1PAF[j,k]=1-1/exp(GEMM1RR[j,k])
    GEMM2PAF[j,k]=1-1/exp(GEMM2RR[j,k])
    GEMM1_lowPAF[j,k]=1-1/exp(GEMM1_lowRR[j,k])
    GEMM2_lowPAF[j,k]=1-1/exp(GEMM2_lowRR[j,k])
    GEMM1_highPAF[j,k]=1-1/exp(GEMM1_highRR[j,k])
    GEMM2_highPAF[j,k]=1-1/exp(GEMM2_highRR[j,k])
  }}

#calculate excess deaths by country:  ED=#deaths * PAF

meanHYBCouED=matrix(0, length(PM), 1)
lclHYBCouED=matrix(0, length(PM), 1)
uclHYBCouED=matrix(0, length(PM), 1)
meanGEMM1CouED=matrix(0, length(PM), 1)
lclGEMM1CouED=matrix(0, length(PM), 1)
uclGEMM1CouED=matrix(0, length(PM), 1)
meanGEMM2CouED=matrix(0, length(PM), 1)
lclGEMM2CouED=matrix(0, length(PM), 1)
uclGEMM2CouED=matrix(0, length(PM), 1)

for (k in 1:length(PM)) {

  meanHYBCouED[k]=NACdeaths[k]*mean(HYBPAF[,k])
  lclHYBCouED[k]=NACdeaths[k]*quantile(HYBPAF[,k], 0.025)
  uclHYBCouED[k]=NACdeaths[k]*quantile(HYBPAF[,k], 0.975)
  meanGEMM1CouED[k]=NACdeaths[k]*mean(GEMM1PAF[,k])
  lclGEMM1CouED[k]=NACdeaths[k]*mean(GEMM1_lowPAF[,k])
  uclGEMM1CouED[k]=NACdeaths[k]*mean(GEMM1_highPAF[,k])
  meanGEMM2CouED[k]=NACdeaths[k]*mean(GEMM2PAF[,k])
  lclGEMM2CouED[k]=NACdeaths[k]*mean(GEMM2_lowPAF[,k])
  uclGEMM2CouED[k]=NACdeaths[k]*mean(GEMM2_highPAF[,k])
}

out=data.frame(pop, PM ,  lclHYBCouED, meanHYBCouED, uclHYBCouED,  lclGEMM1CouED, meanGEMM1CouED, uclGEMM1CouED, lclGEMM2CouED, meanGEMM2CouED, uclGEMM2CouED)

write.csv(out, file="Health_burden.csv",  row.names =F)
