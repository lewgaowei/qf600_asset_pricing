options center linesize=180 pageno=1 pagesize=64;
run;


libname crsp "D:\Data\CRSP";
libname compstat "D:\Data\Compustat";
libname ccm "D:\Data\CCM";
libname ff "D:\Data\FF factors";
run;


data comp1;
	set compstat.funda;
	if indfmt='INDL' and DATAFMT='STD' and POPSRC='D' and CONSOL='C';
	keep datadate gvkey fyear fyr emp csho prcc_f
acchg aco acox act am ao aoloch	aox	ap apalch aqc aqi aqs at bast caps capx	capxv ceq ceql ceqt	ch	che	chech cld2 cld3	cld4 cld5 cogs	cstk cstkcv	cstke dc dclo dcom dcpstk dcs dcvsr dcvsub	dcvt	dd	dd1	dd2	dd3	dd4	dd5	dfs	dfxa	diladj	dilavx	dlc	dlcch	dltis	dlto	dltp	dltr	dltt	dm	dn	do	donr	dp	dpacb	dpacc	dpacli	dpacls	dpacme	dpacnr	dpaco	dpact	dpc	dpvieb	dpvio	dpvir	drc	drlt	ds	dudd	dv	dvc	dvp	dvpa	dvpibb	dvt	dxd2	dxd3	dxd4	dxd5	ebit	ebitda	esopct	esopdlt	esopnr	esopr	esopt	esub	esubc	exre fatb fatc	fate fatl fatn fato fatp fca fiao fincf fopo fopox fopt fsrco fsrct fuseo fuset gdwl gp ib ibadj ibc ibcom icapt idit intan	intc intpn	invch	invfg	invo	invrm	invt	invwip	itcb	itci	ivaco	ivaeq	ivao	ivch	ivncf	ivst	ivstch	lco	lcox	lcoxdr	lct	lifr	lo	loxdr	lse	lt	mib	mii	mrc1	mrc2	mrc3	mrc4	mrc5	mrct	msa	ni	niadj nieci nopi nopio	np	oancf ob oiadp oibdp pi	pidom pifo ppegt ppenb	ppenc ppenli	ppenls	ppenme	ppennr	ppeno	ppent	ppevbb	ppeveb	ppevo	ppevr	prstkc	pstk pstkc pstkl pstkn pstkr pstkrv	rdip re	rea	reajo recch	recco recd rect recta rectr reuna reunr	revt sale seq siv spi sppe sppiv	sstk	tlcf	tstk	tstkc	tstkme	tstkp txach	txbco txc txdb txdba txdbca	txdbcl	txdc txdfed	txdfo txdi	txditc	txds txfed	txfo txndb	txndba	txndbl	txndbr	txo	txp	txpd txr txs txt txw wcap wcapc wcapch xacc xad xdepl xdp xi xido xidoc xint xintd xopr xpp	xpr	xrd	xrent xsga
;
run;

proc sort data=comp1;
	by gvkey datadate;
run;

data comp2;
	set comp1;
	by gvkey datadate;
	firsttwoyears=0;
	if first.gvkey then firsttwoyears=1;
run;

proc sort data=comp2;
	by gvkey descending firsttwoyears datadate;
run;

data comp3;
	set comp2;
	by gvkey descending firsttwoyears datadate;
	if first.firsttwoyears then firsttwoyears=1;
run;

data c1;
	set comp3;
	year=year(datadate);
	if csho=0 then csho=.;
	mktcap=csho*prcc_f; *year end market cap;
run;



* Extract mktcap from CRSP;

data mktcap;
	set crsp.msf;
	year=year(date);
	month=month(date);
	mktcap=abs(prc)*shrout;
	keep date year month permno mktcap;
run;

* December mktcap;

data decmktcap;
	set mktcap;
	crsp_mktcap_12=mktcap;
	if month(date)=12 then output;
	keep permno year crsp_mktcap_12;
run;

* June mktcap;

data junemktcap;
	set mktcap;
	crsp_mktcap_6=mktcap;
	if month(date)=6 then output;
	drop month mktcap;
run;

proc sort data=decmktcap;
	by permno year;
run;


proc sort data=junemktcap;
	by permno year;
run;


data price;
	set crsp.msf;
	price=abs(prc);
	keep permno date price;
run;

proc sort data=price;
	by permno date;
run;


* Monthly stock returns;

data msf;
	set crsp.msf;
	year=year(date);
	month=month(date);
	mktcap=abs(prc)*shrout;
	lperm=lag1(permno);
	lmktcap=lag1(mktcap);
	if permno=lperm then lagmktcap=lmktcap;
	keep date year month permno ret lagmktcap;
run;

* ============================;
* merge with delisting return;
* ============================;
* delisting returns are missing for active issues (with dlstdt=20121231);
* otherwise, missing delisting returns are rare (<1% of delisted firms);
**************************************************************************;

data delist;
	set crsp.msedelist;
	year=year(dlstdt);
	month=month(dlstdt);
	if dlret<-1 then delete;
	keep permno year month dlstdt dlret dlamt;
run;

proc sort data=msf;
	by permno year month;
run;

proc sort data=delist;
	by permno year month;
run;

data msf;
	merge msf(in=in1) delist(in=in2);
	by permno year month;
	if in1 then output;
run;

data msf;
	set msf;
	if ret=. and dlret>=-1 then ret=dlret;
	mindex=year(date)*12+month(date);
	if mindex<23563 or mindex>24240 then delete; *sample period 1963.07-2019.12;
	drop dlret dlamt dlstdt year month mindex;
run;

*SIC code and ff48 industries;

data siccd;
	set crsp.msfhdr;
	siccd=hsiccd;
	keep permno hsiccd siccd hshrcd;
run;


* ff 48 industry classifications downloaded from WRDS;

data ff48;
	set ff.industry48;
	ind=indgrp;
	keep ind sic1 sic2;
run;

proc sql ; 
   create table sicff48 
   as select a.* , b.* 
   from siccd as a, ff48 as b 
   where a.siccd between b.sic1 and b.sic2; 
quit ; 

data sic;
	set sicff48;
	keep permno ind hsiccd hshrcd;
run;

proc sort data=sic;
	by permno;
run;



%macro fs(variable);

* construct ratios;

data t1;
	set c1;
	var=&variable;
	if at>0 then var_a=var/at;
	if act>0 then var_b=var/act;
	if invt>0 then var_c=var/invt;
	if ppent>0 then var_d=var/ppent;
	if lt>0 then var_e=var/lt;
	if lct>0 then var_f=var/lct;
	if dltt>0 then var_g=var/dltt;
	if ceq>0 then var_h=var/ceq;
	if seq>0 then var_i=var/seq;
	if icapt>0 then var_j=var/icapt;
	if sale>0 then var_k=var/sale;
	if cogs>0 then var_l=var/cogs;
	if xsga>0 then var_m=var/xsga;
	if emp>0 then var_n=var/emp;
	if mktcap>0 then var_o=var/mktcap;
run;


proc sort data=t1;
	by gvkey datadate;
run;

* obtain lagged values of variables;

data t2;
	set t1;
	lfyear=lag1(fyear);
	lvar=lag1(var);
	lvar_a=lag1(var_a);
	lvar_b=lag1(var_b);
	lvar_c=lag1(var_c);
	lvar_d=lag1(var_d);
	lvar_e=lag1(var_e);
	lvar_f=lag1(var_f);
	lvar_g=lag1(var_g);
	lvar_h=lag1(var_h);
	lvar_i=lag1(var_i);
	lvar_j=lag1(var_j);
	lvar_k=lag1(var_k);
	lvar_l=lag1(var_l);
	lvar_m=lag1(var_m);
	lvar_n=lag1(var_n);
	lvar_o=lag1(var_o);
	lat=lag1(at);
	lsale=lag1(sale);
	lact=lag1(act);
	linvt=lag1(invt);
	lppent=lag1(ppent);
	llt=lag1(lt);
	ldltt=lag1(dltt);
	lceq=lag1(ceq);
	lseq=lag1(seq);
	llct=lag(lct);
	licapt=lag1(icapt);
	lcogs=lag1(cogs);
	lxsga=lag1(xsga);
	lemp=lag1(emp);
	lmktcap=lag1(mktcap);
	lgvkey=lag1(gvkey);
	if gvkey=lgvkey and fyear=lfyear+1 then do;
	lagvar=lvar;
	lagvar_a=lvar_a;
	lagvar_b=lvar_b;
	lagvar_c=lvar_c;
	lagvar_d=lvar_d;
	lagvar_e=lvar_e;
	lagvar_f=lvar_f;
	lagvar_g=lvar_g;
	lagvar_h=lvar_h;
	lagvar_i=lvar_i;
	lagvar_j=lvar_j;
	lagvar_k=lvar_k;
	lagvar_l=lvar_l;
	lagvar_m=lvar_m;
	lagvar_n=lvar_n;
	lagvar_o=lvar_o;
	lagat=lat;
	lagsale=lsale;
	lagact=lact;
	laginvt=linvt;
	lagppent=lppent;
	laglt=llt;
	laglct=llct;
	lagdltt=ldltt;
	lagceq=lceq;
	lagseq=lseq;
	lagicapt=licapt;
	lagcogs=lcogs;
	lagxsga=lxsga;
	lagemp=lemp;
	lagmktcap=lmktcap;
	end;
	drop lgvkey lfyear lvar lvar_a lvar_b lvar_c lvar_d lvar_e lvar_f lvar_g lvar_h lvar_i lvar_j lvar_k lvar_l lvar_m lvar_n lvar_o lat lsale lact linvt lppent lmktcap lemp lceq lseq lcogs lxsga licapt ldltt llct llt;
run;

* construct changes and percentage changes;

data t3;
	set t2;
	d_var=var-lagvar;
	d_var_a=var_a-lagvar_a;
	d_var_b=var_b-lagvar_b;
	d_var_c=var_c-lagvar_c;
	d_var_d=var_d-lagvar_d;
	d_var_e=var_e-lagvar_e;
	d_var_f=var_f-lagvar_f;
	d_var_g=var_g-lagvar_g;
	d_var_h=var_h-lagvar_h;
	d_var_i=var_i-lagvar_i;
	d_var_j=var_j-lagvar_j;
	d_var_k=var_k-lagvar_k;
	d_var_l=var_l-lagvar_l;
	d_var_m=var_m-lagvar_m;
	d_var_n=var_n-lagvar_n;
	d_var_o=var_o-lagvar_o;
	if lagvar>0 then pd_var=var/lagvar-1;
	if lagvar_a>0 then pd_var_a=var_a/lagvar_a-1;
	if lagvar_b>0 then pd_var_b=var_b/lagvar_b-1;
	if lagvar_c>0 then pd_var_c=var_c/lagvar_c-1;
	if lagvar_d>0 then pd_var_d=var_d/lagvar_d-1;
	if lagvar_e>0 then pd_var_e=var_e/lagvar_e-1;
	if lagvar_f>0 then pd_var_f=var_f/lagvar_f-1;
	if lagvar_g>0 then pd_var_g=var_g/lagvar_g-1;
	if lagvar_h>0 then pd_var_h=var_h/lagvar_h-1;
	if lagvar_i>0 then pd_var_i=var_i/lagvar_i-1;
	if lagvar_j>0 then pd_var_j=var_j/lagvar_j-1;
	if lagvar_k>0 then pd_var_k=var_k/lagvar_k-1;
	if lagvar_l>0 then pd_var_l=var_l/lagvar_l-1;
	if lagvar_m>0 then pd_var_m=var_m/lagvar_m-1;
	if lagvar_n>0 then pd_var_n=var_n/lagvar_n-1;
	if lagvar_o>0 then pd_var_o=var_o/lagvar_o-1;
	if lagat>0 then do;
	d_var_at=d_var/lagat; *change in var divided by 15 lagged variables;
	pd_at=at/lagat-1;
	end;
	if lagsale>0 then do;
	d_var_sale=d_var/lagsale;
	pd_sale=sale/lagsale-1;
	end;
	if lagact>0 then do;
	d_var_act=d_var/lagact; 
	pd_act=act/lagact-1;
	end;
	if laginvt>0 then do;
	d_var_invt=d_var/laginvt;
	pd_invt=invt/laginvt-1;
	end;
	if lagppent>0 then do;
	d_var_ppent=d_var/lagppent; 
	pd_ppent=ppent/lagppent-1;
	end;
	if laglt>0 then do;
	d_var_lt=d_var/laglt;
	pd_lt=lt/laglt-1;
	end;
	if laglct>0 then do;
	d_var_lct=d_var/laglct; 
	pd_lct=lct/laglct-1;
	end;
	if lagdltt>0 then do;
	d_var_dltt=d_var/lagdltt;
	pd_dltt=dltt/lagdltt-1;
	end;
	if lagceq>0 then do;
	d_var_ceq=d_var/lagceq; 
	pd_ceq=ceq/lagceq-1;
	end;
	if lagseq>0 then do;
	d_var_seq=d_var/lagseq;
	pd_seq=seq/lagseq-1;
	end;
	if lagicapt>0 then do;
	d_var_icapt=d_var/lagicapt; 
	pd_icapt=icapt/lagicapt-1;
	end;
	if lagcogs>0 then do;
	d_var_cogs=d_var/lagcogs;
	pd_cogs=cogs/lagcogs-1;
	end;
	if lagxsga>0 then do;
	d_var_xsga=d_var/lagxsga; 
	pd_xsga=xsga/lagxsga-1;
	end;
	if lagemp>0 then do;
	d_var_emp=d_var/lagemp;
	pd_emp=emp/lagemp-1;
	end;
	if lagmktcap>0 then do;
	d_var_mktcap=d_var/lagmktcap;
	pd_mktcap=mktcap/lagmktcap-1;
	end;
	pd_var_at=pd_var-pd_at; *percentage change in var minus percentage change in 15 variables;
	pd_var_sale=pd_var-pd_sale;
	pd_var_act=pd_var-pd_act;
	pd_var_invt=pd_var-pd_invt;
	pd_var_ppent=pd_var-pd_ppent;
	pd_var_lt=pd_var-pd_lt;
	pd_var_lct=pd_var-pd_lct;
	pd_var_dltt=pd_var-pd_dltt;
	pd_var_ceq=pd_var-pd_ceq;
	pd_var_seq=pd_var-pd_seq;
	pd_var_icapt=pd_var-pd_icapt;
	pd_var_cogs=pd_var-pd_cogs;
	pd_var_xsga=pd_var-pd_xsga;
	pd_var_emp=pd_var-pd_emp;
	pd_var_mktcap=pd_var-pd_mktcap;
run;

* remove the first two years of obs;

data t3;
	set t3;
	if firsttwoyears=1 then delete;
	drop lagvar lagvar_a lagvar_b lagvar_c lagvar_d lagvar_e lagvar_f lagvar_g lagvar_h lagvar_i lagvar_j lagvar_k lagvar_l lagvar_m lagvar_n lagvar_o
    acchg aco acox act am ao aoloch	aox	ap apalch aqc aqi aqs at bast caps capx	capxv ceq ceql ceqt	ch	che	chech cld2 cld3	cld4 cld5 cogs	cstk cstkcv	cstke dc dclo dcom dcpstk dcs dcvsr dcvsub	dcvt	dd	dd1	dd2	dd3	dd4	dd5	dfs	dfxa	diladj	dilavx	dlc	dlcch	dltis	dlto	dltp	dltr	dltt	dm	dn	do	donr	dp	dpacb	dpacc	dpacli	dpacls	dpacme	dpacnr	dpaco	dpact	dpc	dpvieb	dpvio	dpvir	drc	drlt ds	dudd dv	dvc	dvp	dvpa	dvpibb	dvt	dxd2	dxd3	dxd4	dxd5	ebit	ebitda	esopct	esopdlt	esopnr	esopr	esopt	esub	esubc	exre fatb fatc	fate fatl fatn fato fatp fca fiao fincf fopo fopox fopt fsrco fsrct fuseo fuset gdwl gp ib ibadj ibc ibcom icapt idit intan	intc intpn	invch	invfg	invo	invrm	invt	invwip	itcb	itci	ivaco	ivaeq	ivao	ivch	ivncf	ivst	ivstch	lco	lcox	lcoxdr	lct	lifr	lo	loxdr	lse	lt	mib	mii	mrc1	mrc2	mrc3	mrc4	mrc5	mrct	msa	ni	niadj nieci nopi nopio	np	oancf ob oiadp oibdp pi	pidom pifo ppegt ppenb	ppenc ppenli	ppenls	ppenme	ppennr	ppeno	ppent	ppevbb	ppeveb	ppevo	ppevr	prstkc	pstk pstkc pstkl pstkn pstkr pstkrv	rdip re	rea	reajo recch	recco recd rect recta rectr reuna reunr	revt sale seq siv spi sppe sppiv	sstk	tlcf	tstk	tstkc	tstkme	tstkp txach	txbco txc txdb txdba txdbca	txdbcl	txdc txdfed	txdfo txdi	txditc	txds txfed	txfo txndb	txndba	txndbl	txndbr	txo	txp	txpd txr txs txt txw wcap wcapc wcapch xacc xad xdepl xdp xi xido xidoc xint xintd xopr xpp	xpr	xrd	xrent xsga;
run;


* ===================================================================;
* match with permno using the CCM Linktable;
* ====================================================================;

data link;
	set ccm.ccmxpf_linktable;
	permno=lpermno;
	where linktype in ("LU", "LC", "LN", "LS", "LX") and usedflag=1;
	keep permno gvkey linkdt linkenddt;
run;

proc sql;
	create table t4 as select *
	from t3 as a, link as b
	where a.gvkey = b.gvkey and
	(b.linkdt <= a.datadate or b.linkdt= .B) and
	(a.datadate <= b.linkenddt or b.linkenddt = .E);
quit;
run;


* Merge Compustat variables with CRSP December mktcap;

proc sort data=t4;
	by permno year;
run;

data t5;
	merge t4(in=in1) decmktcap(in=in2);
	by permno year;
	if in1 then output;
run;

* merge with next June mktcap;
data t6;
	set t5;
	year=year+1;
run;

proc sort data=t6;
	by permno year;
run;

data t7;
	merge t6(in=in1) junemktcap(in=in2);
	by permno year;
	if in1 then output;
run;


* form portfolios and merge with crsp returns;

* read and clean up the data;

data a0;
	set t7;
	if date=. then delete; * delete if next June market cap not available;
run;

title " ";
run;

* merge with crsp price;

proc sort data=a0;
	by permno date;
run;


data a1;
	merge a0(in=in1) price; 
	by permno date;
	if in1;
run;

*merge with sic code and ff industries;

data a2;
	merge a1(in=in1) sic(in=in2);
	by permno;
	if in1 and in2;
run;

proc sort data=a2;
	by date;
run;


* form anomaly variable portfolios;

data a3;
	set a2;
	if hshrcd^=10 and hshrcd^=11 then delete;
	if price<1 then delete;
	if year(date)<1963 then delete; *sample period 1963-2019;
	if year(date)>2019 then delete;
	if hsiccd>=6000 and hsiccd<7000 then delete;
run;

* # of non-missing obs;

proc means data=a3 n nmiss;
	var var;
	by date;
	ods output summary=a3out;
run;

data nmiss;
	set a3out;
	length var $ 10;
	var="&variable";
run;

proc append base=nmiss0_19632019 data=nmiss;
run;


proc sort data=a3;
	by date;
run;

*76 fs ratios;

proc rank data=a3 group=10 out=a33 ties=low;
	var var_a var_b var_c var_d var_e var_f var_g var_h var_i var_j var_k var_l var_m var_n var_o
	d_var_a d_var_b d_var_c d_var_d d_var_e d_var_f d_var_g d_var_h d_var_i d_var_j d_var_k d_var_l d_var_m d_var_n d_var_o
	pd_var_a pd_var_b pd_var_c pd_var_d pd_var_e pd_var_f pd_var_g pd_var_h pd_var_i pd_var_j pd_var_k pd_var_l pd_var_m pd_var_n pd_var_o 
	d_var_at d_var_sale d_var_act  d_var_invt d_var_ppent d_var_lt  d_var_lct d_var_dltt d_var_ceq d_var_seq d_var_icapt d_var_cogs d_var_xsga d_var_emp d_var_mktcap  
	pd_var pd_var_at pd_var_sale pd_var_act pd_var_invt pd_var_ppent pd_var_lt  pd_var_lct pd_var_dltt pd_var_ceq pd_var_seq pd_var_icapt pd_var_cogs pd_var_xsga pd_var_emp pd_var_mktcap;
	by date;
	ranks var_a var_b var_c var_d var_e var_f var_g var_h var_i var_j var_k var_l var_m var_n var_o
	d_var_a d_var_b d_var_c d_var_d d_var_e d_var_f d_var_g d_var_h d_var_i d_var_j d_var_k d_var_l d_var_m d_var_n d_var_o
	pd_var_a pd_var_b pd_var_c pd_var_d pd_var_e pd_var_f pd_var_g pd_var_h pd_var_i pd_var_j pd_var_k pd_var_l pd_var_m pd_var_n pd_var_o 
	d_var_at d_var_sale d_var_act  d_var_invt d_var_ppent d_var_lt  d_var_lct d_var_dltt d_var_ceq d_var_seq d_var_icapt d_var_cogs d_var_xsga d_var_emp d_var_mktcap  
	pd_var pd_var_at pd_var_sale pd_var_act pd_var_invt pd_var_ppent pd_var_lt  pd_var_lct pd_var_dltt pd_var_ceq pd_var_seq pd_var_icapt pd_var_cogs pd_var_xsga pd_var_emp pd_var_mktcap;
run;

data a33;
	set a33;
	rename date=form_date;
run;

* merge with stock returns of next (e.g., 12) months;

proc sql;
    create table a333
    as select distinct a.*, b.*,
	intck('month',a.form_date,b.date) as eventmonth 
    from a33 as a, msf as b
    where a.permno=b.permno 
    and 0<intck('month',a.form_date,b.date)<=12;
quit;


%portfolio(var_a); run;

%portfolio(var_b); run;
%portfolio(var_c); run;
%portfolio(var_d); run;
%portfolio(var_e); run;
%portfolio(var_f); run;
%portfolio(var_g); run;
%portfolio(var_h); run;
%portfolio(var_i); run;
%portfolio(var_j); run;
%portfolio(var_k); run;
%portfolio(var_l); run;
%portfolio(var_m); run;
%portfolio(var_n); run;
%portfolio(var_o); run;

%portfolio(d_var_a); run;
%portfolio(d_var_b); run;
%portfolio(d_var_c); run;
%portfolio(d_var_d); run;
%portfolio(d_var_e); run;
%portfolio(d_var_f); run;
%portfolio(d_var_g); run;
%portfolio(d_var_h); run;
%portfolio(d_var_i); run;
%portfolio(d_var_j); run;
%portfolio(d_var_k); run;
%portfolio(d_var_l); run;
%portfolio(d_var_m); run;
%portfolio(d_var_n); run;
%portfolio(d_var_o); run;

%portfolio(pd_var); run;

%portfolio(pd_var_a); run;
%portfolio(pd_var_b); run;
%portfolio(pd_var_c); run;
%portfolio(pd_var_d); run;
%portfolio(pd_var_e); run;
%portfolio(pd_var_f); run;
%portfolio(pd_var_g); run;
%portfolio(pd_var_h); run;
%portfolio(pd_var_i); run;
%portfolio(pd_var_j); run;
%portfolio(pd_var_k); run;
%portfolio(pd_var_l); run;
%portfolio(pd_var_m); run;
%portfolio(pd_var_n); run;
%portfolio(pd_var_o); run;

%portfolio(d_var_at);run;
%portfolio(d_var_sale);run;
%portfolio(d_var_act);run;
%portfolio(d_var_invt);run;
%portfolio(d_var_ppent);run;
%portfolio(d_var_lt);run;
%portfolio(d_var_lct);run;
%portfolio(d_var_dltt);run;
%portfolio(d_var_ceq);run;
%portfolio(d_var_seq);run;
%portfolio(d_var_icapt);run;
%portfolio(d_var_cogs);run;
%portfolio(d_var_xsga);run;
%portfolio(d_var_emp);run;
%portfolio(d_var_mktcap);run;

%portfolio(pd_var_at);run;
%portfolio(pd_var_sale);run;
%portfolio(pd_var_act);run;
%portfolio(pd_var_invt);run;
%portfolio(pd_var_ppent);run;
%portfolio(pd_var_lt);run;
%portfolio(pd_var_lct);run;
%portfolio(pd_var_dltt);run;
%portfolio(pd_var_ceq);run;
%portfolio(pd_var_seq);run;
%portfolio(pd_var_icapt);run;
%portfolio(pd_var_cogs);run;
%portfolio(pd_var_xsga);run;
%portfolio(pd_var_emp);run;
%portfolio(pd_var_mktcap);run;


%mend fs;run;


%macro portfolio(anomalyvariable); 

*************************************************************************************
Calculate Equal-weight and Value-weight Average Monthly Returns
*************************************************************************************;

data a4;
	set a333;
	decile=&anomalyvariable;
	if decile=. then delete; *delete those without decile rankings;
	keep date decile ret crsp_mktcap_6;
run;

* Decile portfolio average monthly return;

proc sort data=a4;
	by date decile; 
run;

*average return for each decile and each month;

proc means data = a4 noprint;
    by date decile;
    var ret;
    output out=ewout mean=ret;
run;

proc means data = a4 noprint;
    by date decile;
    var ret;
	weight crsp_mktcap_6;
    output out=vwout mean=ret;
run;

proc transpose data=ewout out=decile_ew
  (rename = (_0=d1_ew _1=d2_ew _2=d3_ew _3=d4_ew _4=d5_ew
	_5=d6_ew _6=d7_ew _7=d8_ew _8=d9_ew _9=d10_ew));
   by date;
   id decile;
   var ret;
run;

proc transpose data=vwout out=decile_vw
  (rename = (_0=d1_vw _1=d2_vw _2=d3_vw _3=d4_vw _4=d5_vw
	_5=d6_vw _6=d7_vw _7=d8_vw _8=d9_vw _9=d10_vw));
   by date;
   id decile;
   var ret;
run;

data decile_ew;
	set decile_ew;
	ddiff_ew=d10_ew-d1_ew;
run;

data decile_vw;
	set decile_vw;
	ddiff_vw=d10_vw-d1_vw;
run;

data decile;
	merge decile_ew decile_vw;
	by date;
run;

data decile;
	set decile;
	drop _name_ _label_;
	length anomalyvariable $ 15;
	length var $ 10;
	anomalyvariable="&anomalyvariable";
	var="&variable";
run;

proc append base=decile0_19632019 data=decile;
run;

dm 'log; clear; output; clear'; 


%mend portfolio;

%fs(acchg);run;
%fs(aco);run;
%fs(acox);run;
%fs(act);run;
%fs(am);run;
%fs(ao);run;
%fs(aoloch);run;
%fs(aox);run;
%fs(ap);run;
%fs(apalch);run;
%fs(aqc);run;
%fs(aqi);run;
%fs(aqs);run;
%fs(at);run;
%fs(bast);run;
%fs(caps);run;
%fs(capx);run;
%fs(capxv);run;
%fs(ceq);run;
%fs(ceql);run;
%fs(ceqt);run;
%fs(ch);run;
%fs(che);run;
%fs(chech);run;
%fs(cld2);run;
%fs(cld3);run;
%fs(cld4);run;
%fs(cld5);run;
%fs(cogs);run;
%fs(cstk);run;
%fs(cstkcv);run;
%fs(cstke);run;
%fs(dc);run;
%fs(dclo);run;
%fs(dcom);run;
%fs(dcpstk);run;
%fs(dcvsr);run;
%fs(dcvsub);run;
%fs(dcvt);run;
%fs(dd);run;
%fs(dd1);run;
%fs(dd2);run;
%fs(dd3);run;
%fs(dd4);run;
%fs(dd5);run;
%fs(dfs);run;
%fs(dfxa);run;
%fs(diladj);run;
%fs(dilavx);run;
%fs(dlc);run;
%fs(dlcch);run;
%fs(dltis);run;
%fs(dlto);run;
%fs(dltp);run;
%fs(dltr);run;
%fs(dltt);run;
%fs(dm);run;
%fs(dn);run;
%fs(do);run;
%fs(donr);run;
%fs(dp);run;
%fs(dpact);run;
%fs(dpc);run;
%fs(dpvieb);run;
%fs(dpvio);run;
%fs(dpvir);run;
%fs(drc);run;
%fs(ds);run;
%fs(dudd);run;
%fs(dv);run;
%fs(dvc);run;
%fs(dvp);run;
%fs(dvpa);run;
%fs(dvpibb);run;
%fs(dvt);run;
%fs(dxd2);run;
%fs(dxd3);run;
%fs(dxd4);run;
%fs(dxd5);run;
%fs(ebit);run;
%fs(ebitda);run;
%fs(esopct);run;
%fs(esopdlt);run;
%fs(esopt);run;
%fs(esub);run;
%fs(esubc);run;
%fs(exre);run;
%fs(fatb);run;
%fs(fatc);run;
%fs(fate);run;
%fs(fatl);run;
%fs(fatn);run;
%fs(fato);run;
%fs(fatp);run;
%fs(fiao);run;
%fs(fincf);run;
%fs(fopo);run;
%fs(fopox);run;
%fs(fopt);run;
%fs(fsrco);run;
%fs(fsrct);run;
%fs(fuseo);run;
%fs(fuset);run;
%fs(gdwl);run;
%fs(gp);run;
%fs(ib);run;
%fs(ibadj);run;
%fs(ibc);run;
%fs(ibcom);run;
%fs(icapt);run;
%fs(idit);run;
%fs(intan);run;
%fs(intc);run;
%fs(intpn);run;
%fs(invch);run;
%fs(invfg);run;
%fs(invo);run;
%fs(invrm);run;
%fs(invt);run;
%fs(invwip);run;
%fs(itcb);run;
%fs(itci);run;
%fs(ivaco);run;
%fs(ivaeq);run;
%fs(ivao);run;
%fs(ivch);run;
%fs(ivncf);run;
%fs(ivst);run;
%fs(ivstch);run;
%fs(lco);run;
%fs(lcox);run;
%fs(lcoxdr);run;
%fs(lct);run;
%fs(lifr);run;
%fs(lo);run;
%fs(lt);run;
%fs(mib);run;
%fs(mii);run;
%fs(mrc1);run;
%fs(mrc2);run;
%fs(mrc3);run;
%fs(mrc4);run;
%fs(mrc5);run;
%fs(mrct);run;
%fs(msa);run;
%fs(ni);run;
%fs(niadj);run;
%fs(nieci);run;
%fs(nopi);run;
%fs(nopio);run;
%fs(np);run;
%fs(oancf);run;
%fs(ob);run;
%fs(oiadp);run;
%fs(pi);run;
%fs(pidom);run;
%fs(pifo);run;
%fs(ppegt);run;
%fs(ppenb);run;
%fs(ppenc);run;
%fs(ppenli);run;
%fs(ppenme);run;
%fs(ppennr);run;
%fs(ppeno);run;
%fs(ppent);run;
%fs(ppevbb);run;
%fs(ppeveb);run;
%fs(ppevo);run;
%fs(ppevr);run;
%fs(prstkc);run;
%fs(pstk);run;
%fs(pstkc);run;
%fs(pstkl);run;
%fs(pstkn);run;
%fs(pstkr);run;
%fs(pstkrv);run;
%fs(rdip);run;
%fs(re);run;
%fs(rea);run;
%fs(reajo);run;
%fs(recch);run;
%fs(recco);run;
%fs(recd);run;
%fs(rect);run;
%fs(recta);run;
%fs(rectr);run;
%fs(reuna);run;
%fs(sale);run;
%fs(seq);run;
%fs(siv);run;
%fs(spi);run;
%fs(sppe);run;
%fs(sppiv);run;
%fs(sstk);run;
%fs(tlcf);run;
%fs(tstk);run;
%fs(tstkc);run;
%fs(tstkp);run;
%fs(txach);run;
%fs(txbco);run;
%fs(txc);run;
%fs(txdb);run;
%fs(txdba);run;
%fs(txdbca);run;
%fs(txdbcl);run;
%fs(txdc);run;
%fs(txdfed);run;
%fs(txdfo);run;
%fs(txdi);run;
%fs(txditc);run;
%fs(txds);run;
%fs(txfed);run;
%fs(txfo);run;
%fs(txndb);run;
%fs(txndba);run;
%fs(txndbl);run;
%fs(txndbr);run;
%fs(txo);run;
%fs(txp);run;
%fs(txpd);run;
%fs(txr);run;
%fs(txs);run;
%fs(txt);run;
%fs(txw);run;
%fs(wcap);run;
%fs(wcapc);run;
%fs(wcapch);run;
%fs(xacc);run;
%fs(xad);run;
%fs(xdepl);run;
%fs(xi);run;
%fs(xido);run;
%fs(xidoc);run;
%fs(xint);run;
%fs(xopr);run;
%fs(xpp);run;
%fs(xpr);run;
%fs(xrd);run;
%fs(xrent);run;
%fs(xsga);run;
