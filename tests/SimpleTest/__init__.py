
import remus
import csv

__manifest__ = [ "__init__.py" ]

class PipelineRoot(remus.SubmitTarget):
	
	def run(self, params):
		self.addChildTarget( 'tableScan', GenerateTable('inputTable') )
		self.addFollowTarget( 'tableMap', TableMap( 'tableScan/inputTable') )
	

startText = """probe	exp_1	exp_2	exp_3	exp_4	exp_5	exp_6	exp_7	exp_8	exp_9
gene_1	0.262491117231548	0.0196498045697808	0.216511648381129	0.109295522794127	0.168183598667383	0.176425657933578	0.478054876904935	0.511331603862345	0.253156918566674
gene_2	0.524982234463096	0.0392996091395617	0.433023296762258	0.218591045588255	0.336367197334766	0.352851315867156	0.95610975380987	1.02266320772469	0.506313837133348
gene_3	0.787473351694643	0.0589494137093425	0.649534945143387	0.327886568382382	0.50455079600215	0.529276973800734	1.43416463071480	1.53399481158704	0.759470755700022
gene_4	1.04996446892619	0.0785992182791233	0.866046593524516	0.43718209117651	0.672734394669533	0.705702631734312	1.91221950761974	2.04532641544938	1.01262767426670
gene_5	1.31245558615774	0.0982490228489041	1.08255824190564	0.546477613970637	0.840917993336916	0.88212828966789	2.39027438452467	2.55665801931173	1.26578459283337
gene_6	1.57494670338929	0.117898827418685	1.29906989028677	0.655773136764765	1.0091015920043	1.05855394760147	2.86832926142961	3.06798962317407	1.51894151140004
gene_7	1.83743782062083	0.137548631988466	1.51558153866790	0.765068659558892	1.17728519067168	1.23497960553505	3.34638413833454	3.57932122703642	1.77209842996672
gene_8	2.09992893785238	0.157198436558247	1.73209318704903	0.87436418235302	1.34546878933907	1.41140526346862	3.82443901523948	4.09065283089876	2.02525534853339
gene_9	2.36242005508393	0.176848241128027	1.94860483543016	0.983659705147147	1.51365238800645	1.5878309214022	4.30249389214441	4.60198443476111	2.27841226710007
gene_10	2.62491117231548	0.196498045697808	2.16511648381129	1.09295522794127	1.68183598667383	1.76425657933578	4.78054876904935	5.11331603862345	2.53156918566674
gene_11	26.2491117231548	0.216147850267589	2.38162813219242	1.20225075073540	1.85001958534122	1.94068223726936	5.25860364595428	5.6246476424858	2.78472610423341
gene_12	25.9866206059232	0.23579765483737	2.59813978057355	1.31154627352953	2.0182031840086	2.11710789520293	5.73665852285922	6.13597924634814	3.03788302280009
gene_13	25.7241294886917	0.255447459407151	2.81465142895468	1.42084179632366	2.18638678267598	2.29353355313651	6.21471339976415	6.64731085021049	3.29103994136676
gene_14	25.4616383714601	0.275097263976932	3.03116307733580	1.53013731911778	2.35457038134336	2.46995921107009	6.69276827666909	7.15864245407283	3.54419685993344
gene_15	25.1991472542286	0.294747068546712	3.24767472571693	1.63943284191191	2.52275398001075	2.64638486900367	7.17082315357402	7.66997405793518	3.79735377850011
gene_16	24.9366561369970	0.314396873116493	3.46418637409806	1.74872836470604	2.69093757867813	2.82281052693725	7.64887803047895	8.18130566179752	4.05051069706678
gene_17	24.6741650197655	0.334046677686274	3.68069802247919	1.85802388750017	2.85912117734551	2.99923618487082	8.12693290738389	8.69263726565987	4.30366761563346
gene_18	24.4116739025339	0.353696482256055	3.89720967086032	1.96731941029429	3.0273047760129	3.1756618428044	8.60498778428882	9.20396886952221	4.55682453420013
gene_19	24.1491827853024	0.373346286825836	4.11372131924145	2.07661493308842	3.19548837468028	3.35208750073798	9.08304266119376	9.71530047338456	4.80998145276681
gene_20	23.8866916680709	0.392996091395617	4.33023296762258	2.18591045588255	3.36367197334766	3.52851315867156	9.5610975380987	10.2266320772469	5.06313837133348
gene_21	23.6242005508393	1.96498045697808	4.54674461600371	2.29520597867668	3.53185557201505	3.70493881660514	10.0391524150036	10.7379636811092	5.31629528990015
gene_22	23.3617094336078	1.94533065240830	4.76325626438484	2.40450150147080	3.70003917068243	3.88136447453871	10.5172072919086	11.2492952849716	5.56945220846683
gene_23	23.0992183163762	1.92568084783852	4.97976791276596	2.51379702426493	3.86822276934981	4.05779013247229	10.9952621688135	11.7606268888339	5.8226091270335
gene_24	22.8367271991447	1.90603104326874	5.19627956114709	2.62309254705906	4.0364063680172	4.23421579040587	11.4733170457184	12.2719584926963	6.07576604560018
gene_25	22.5742360819131	1.88638123869896	5.41279120952822	2.73238806985319	4.20458996668458	4.41064144833945	11.9513719226234	12.7832900965586	6.32892296416685
gene_26	22.3117449646816	1.86673143412918	5.62930285790935	2.84168359264731	4.37277356535196	4.58706710627303	12.4294267995283	13.2946217004210	6.58207988273352
gene_27	22.04925384745	1.84708162955940	5.84581450629048	2.95097911544144	4.54095716401935	4.7634927642066	12.9074816764332	13.8059533042833	6.8352368013002
gene_28	21.7867627302185	1.82743182498962	6.06232615467161	3.06027463823557	4.70914076268673	4.93991842214018	13.3855365533382	14.3172849081457	7.08839371986687
gene_29	21.5242716129869	1.80778202041984	6.27883780305274	3.1695701610297	4.87732436135411	5.11634408007376	13.8635914302431	14.828616512008	7.34155063843355
gene_30	21.2617804957554	1.78813221585006	6.49534945143387	3.27886568382382	5.0455079600215	5.29276973800734	14.3416463071480	15.3399481158704	7.59470755700022
gene_31	20.9992893785238	1.76848241128027	21.6511648381129	3.38816120661795	5.21369155868888	5.46919539594091	14.8197011840530	15.8512797197327	7.8478644755669
gene_32	20.7367982612923	1.74883260671049	21.4346531897318	3.49745672941208	5.38187515735626	5.64562105387449	15.2977560609579	16.3626113235950	8.10102139413357
gene_33	20.4743071440607	1.72918280214071	21.2181415413506	3.60675225220621	5.55005875602365	5.82204671180807	15.7758109378628	16.8739429274574	8.35417831270024
gene_34	20.2118160268292	1.70953299757093	21.0016298929695	3.71604777500033	5.71824235469103	5.99847236974165	16.2538658147678	17.3852745313197	8.60733523126692
gene_35	19.9493249095976	1.68988319300115	20.7851182445884	3.82534329779446	5.88642595335841	6.17489802767523	16.7319206916727	17.8966061351821	8.86049214983359
gene_36	19.6868337923661	1.67023338843137	20.5686065962072	3.93463882058859	6.0546095520258	6.3513236856088	17.2099755685776	18.4079377390444	9.11364906840026
gene_37	19.4243426751345	1.65058358386159	20.3520949478261	4.04393434338272	6.22279315069318	6.52774934354238	17.6880304454826	18.9192693429068	9.36680598696694
gene_38	19.161851557903	1.63093377929181	20.135583299445	4.15322986617684	6.39097674936056	6.70417500147596	18.1660853223875	19.4306009467691	9.61996290553361
gene_39	18.8993604406714	1.61128397472203	19.9190716510639	4.26252538897097	6.55916034802794	6.88060065940954	18.6441401992925	19.9419325506315	9.87311982410029
gene_40	18.6368693234399	1.59163417015225	19.7025600026827	4.3718209117651	6.72734394669533	7.05702631734312	19.1221950761974	20.4532641544938	10.1262767426670
gene_41	18.3743782062083	1.57198436558247	19.4860483543016	10.9295522794127	6.89552754536271	7.2334519752767	19.6002499531023	20.9645957583562	10.3794336612336
gene_42	18.1118870889768	1.55233456101269	19.2695367059205	10.8202567566186	7.06371114403009	7.40987763321027	20.0783048300073	21.4759273622185	10.6325905798003
gene_43	17.8493959717453	1.53268475644290	19.0530250575393	10.7109612338245	7.23189474269748	7.58630329114385	20.5563597069122	21.9872589660808	10.8857474983670
gene_44	17.5869048545137	1.51303495187312	18.8365134091582	10.6016657110304	7.40007834136486	7.76272894907743	21.0344145838171	22.4985905699432	11.1389044169337
gene_45	17.3244137372822	1.49338514730334	18.6200017607771	10.4923701882362	7.56826194003224	7.939154607011	21.5124694607221	23.0099221738055	11.3920613355003
gene_46	17.0619226200506	1.47373534273356	18.4034901123960	10.3830746654421	7.73644553869963	8.11558026494458	21.990524337627	23.5212537776679	11.645218254067
gene_47	16.7994315028191	1.45408553816378	18.1869784640148	10.2737791426480	7.90462913736701	8.29200592287816	22.4685792145319	24.0325853815302	11.8983751726337
gene_48	16.5369403855875	1.434435733594	17.9704668156337	10.1644836198539	8.0728127360344	8.46843158081174	22.9466340914369	24.5439169853926	12.1515320912004
gene_49	16.2744492683560	1.41478592902422	17.7539551672526	10.0551880970597	8.24099633470178	8.64485723874532	23.4246889683418	25.0552485892549	12.4046890097670
gene_50	16.0119581511244	1.39513612445444	17.5374435188714	9.9458925742656	8.40917993336916	8.8212828966789	23.9027438452467	25.5665801931173	12.6578459283337
gene_51	15.7494670338929	1.37548631988466	17.3209318704903	9.83659705147147	16.8183598667383	8.99770855461247	24.3807987221517	26.0779117969796	12.9110028469004
gene_52	15.4869759166613	1.35583651531488	17.1044202221092	9.72730152867734	16.6501762680709	9.17413421254605	24.8588535990566	26.5892434008420	13.1641597654670
gene_53	15.2244847994298	1.33618671074510	16.8879085737281	9.61800600588322	16.4819926694036	9.35055987047963	25.3369084759615	27.1005750047043	13.4173166840337
gene_54	14.9619936821982	1.31653690617532	16.6713969253469	9.50871048308909	16.3138090707362	9.5269855284132	25.8149633528665	27.6119066085666	13.6704736026004
gene_55	14.6995025649667	1.29688710160553	16.4548852769658	9.39941496029496	16.1456254720688	9.70341118634678	26.2930182297714	28.123238212429	13.9236305211671
gene_56	14.4370114477351	1.27723729703575	16.2383736285847	9.29011943750083	15.9774418734014	9.87983684428036	26.7710731066763	28.6345698162913	14.1767874397337
gene_57	14.1745203305036	1.25758749246597	16.0218619802035	9.1808239147067	15.8092582747340	10.0562625022139	27.2491279835813	29.1459014201537	14.4299443583004
gene_58	13.9120292132720	1.23793768789619	15.8053503318224	9.07152839191258	15.6410746760666	10.2326881601475	27.7271828604862	29.657233024016	14.6831012768671
gene_59	13.6495380960405	1.21828788332641	15.5888386834413	8.96223286911845	15.4728910773993	10.4091138180811	28.2052377373911	30.1685646278784	14.9362581954338
gene_60	13.3870469788089	1.19863807875663	15.3723270350602	8.85293734632432	15.3047074787319	10.5855394760147	28.6832926142961	30.6798962317407	15.1894151140004
gene_61	13.1245558615774	1.17898827418685	15.1558153866790	8.7436418235302	15.1365238800645	17.6425657933578	29.161347491201	31.1912278356031	15.4425720325671
gene_62	12.8620647443458	1.15933846961707	14.9393037382979	8.63434630073607	14.9683402813971	17.4661401354242	29.6394023681059	31.7025594394654	15.6957289511338
gene_63	12.5995736271143	1.13968866504729	14.7227920899168	8.52505077794194	14.8001566827297	17.2897144774906	30.1174572450109	32.2138910433277	15.9488858697005
gene_64	12.3370825098827	1.12003886047751	14.5062804415356	8.41575525514781	14.6319730840623	17.1132888195571	30.5955121219158	32.7252226471901	16.2020427882671
gene_65	12.0745913926512	1.10038905590773	14.2897687931545	8.30645973235369	14.4637894853950	16.9368631616235	31.0735669988208	33.2365542510524	16.4551997068338
gene_66	11.8121002754197	1.08073925133795	14.0732571447734	8.19716420955956	14.2956058867276	16.7604375036899	31.5516218757257	33.7478858549148	16.7083566254005
gene_67	11.5496091581881	1.06108944676816	13.8567454963923	8.08786868676543	14.1274222880602	16.5840118457563	32.0296767526306	34.2592174587771	16.9615135439672
gene_68	11.2871180409566	1.04143964219838	13.6402338480111	7.9785731639713	13.9592386893928	16.4075861878227	32.5077316295356	34.7705490626395	17.2146704625338
gene_69	11.024626923725	1.02178983762860	13.42372219963	7.86927764117718	13.7910550907254	16.2311605298892	32.9857865064405	35.2818806665018	17.4678273811005
gene_70	10.7621358064935	1.00214003305882	13.2072105512489	7.75998211838305	13.6228714920580	16.0547348719556	33.4638413833454	35.7932122703642	17.7209842996672
gene_71	10.4996446892619	0.982490228489041	12.9906989028677	7.65068659558892	13.4546878933907	15.878309214022	47.8054876904935	36.3045438742265	17.9741412182339
gene_72	10.2371535720304	0.96284042391926	12.7741872544866	7.5413910727948	13.2865042947233	15.7018835560884	47.3274328135885	36.8158754780889	18.2272981368005
gene_73	9.97466245479882	0.94319061934948	12.5576756061055	7.43209555000067	13.1183206960559	15.5254578981549	46.8493779366836	37.3272070819512	18.4804550553672
gene_74	9.71217133756727	0.923540814779699	12.3411639577243	7.32280002720654	12.9501370973885	15.3490322402213	46.3713230597787	37.8385386858135	18.7336119739339
gene_75	9.44968022033572	0.903891010209918	12.1246523093432	7.21350450441241	12.7819534987211	15.1726065822877	45.8932681828737	38.3498702896759	18.9867688925005
gene_76	9.18718910310417	0.884241205640137	11.9081406609621	7.10420898161829	12.6137699000537	14.9961809243541	45.4152133059688	38.8612018935382	19.2399258110672
gene_77	8.92469798587263	0.864591401070356	11.6916290125810	6.99491345882416	12.4455863013864	14.8197552664205	44.9371584290639	39.3725334974006	19.4930827296339
gene_78	8.66220686864108	0.844941596500576	11.4751173641998	6.88561793603003	12.2774027027190	14.6433296084870	44.4591035521589	39.8838651012629	19.7462396482006
gene_79	8.39971575140953	0.825291791930795	11.2586057158187	6.7763224132359	12.1092191040516	14.4669039505534	43.981048675254	40.3951967051253	19.9993965667672
gene_80	8.13722463417798	0.805641987361014	11.0420940674376	6.66702689044178	11.9410355053842	14.2904782926198	43.5029937983491	40.9065283089876	20.2525534853339
gene_81	7.87473351694643	0.785992182791233	10.8255824190564	6.55773136764765	11.7728519067168	14.1140526346862	43.0249389214441	51.1331603862345	20.5057104039006
gene_82	7.61224239971489	0.766342378221452	10.6090707706753	6.44843584485352	11.6046683080494	13.9376269767527	42.5468840445392	50.6218287823722	20.7588673224673
gene_83	7.34975128248334	0.746692573651671	10.3925591222942	6.3391403220594	11.4364847093821	13.7612013188191	42.0688291676342	50.1104971785098	21.0120242410339
gene_84	7.08726016525179	0.72704276908189	10.1760474739131	6.22984479926527	11.2683011107147	13.5847756608855	41.5907742907293	49.5991655746475	21.2651811596006
gene_85	6.82476904802024	0.70739296451211	9.95953582553193	6.12054927647114	11.1001175120473	13.4083500029519	41.1127194138244	49.0878339707851	21.5183380781673
gene_86	6.5622779307887	0.687743159942329	9.7430241771508	6.01125375367701	10.9319339133799	13.2319243450183	40.6346645369194	48.5765023669228	21.7714949967340
gene_87	6.29978681355715	0.668093355372548	9.52651252876967	5.90195823088288	10.7637503147125	13.0554986870848	40.1566096600145	48.0651707630605	22.0246519153006
gene_88	6.0372956963256	0.648443550802767	9.31000088038854	5.79266270808876	10.5955667160451	12.8790730291512	39.6785547831096	47.5538391591981	22.2778088338673
gene_89	5.77480457909405	0.628793746232986	9.09348923200741	5.68336718529463	10.4273831173778	12.7026473712176	39.2004999062046	47.0425075553358	22.530965752434
gene_90	5.5123134618625	0.609143941663206	8.87697758362629	5.5740716625005	10.2591995187104	12.5262217132840	38.7224450292997	46.5311759514734	22.7841226710007
gene_91	5.24982234463096	0.589494137093425	8.66046593524516	5.46477613970637	10.091015920043	12.3497960553505	38.2443901523948	46.0198443476111	25.3156918566674
gene_92	4.98733122739941	0.569844332523644	8.44395428686403	5.35548061691225	9.9228323213756	12.1733703974169	37.7663352754898	45.5085127437487	25.0625349381007
gene_93	4.72484011016786	0.550194527953863	8.2274426384829	5.24618509411812	9.75464872270823	11.9969447394833	37.2882803985849	44.9971811398864	24.8093780195341
gene_94	4.46234899293631	0.530544723384082	8.01093099010177	5.13688957132399	9.58646512404084	11.8205190815497	36.81022552168	44.485849536024	24.5562211009674
gene_95	4.19985787570477	0.510894918814301	7.79441934172064	5.02759404852986	9.41828152537346	11.6440934236161	36.332170644775	43.9745179321617	24.3030641824007
gene_96	3.93736675847322	0.491245114244521	7.57790769333951	4.91829852573574	9.25009792670608	11.4676677656826	35.8541157678701	43.4631863282993	24.0499072638340
gene_97	3.67487564124167	0.47159530967474	7.36139604495838	4.80900300294161	9.0819143280387	11.2912421077490	35.3760608909652	42.951854724437	23.7967503452674
gene_98	3.41238452401012	0.451945505104959	7.14488439657725	4.69970748014748	8.91373072937131	11.1148164498154	34.8980060140602	42.4405231205747	23.5435934267007
gene_99	3.14989340677857	0.432295700535178	6.92837274819613	4.59041195735335	8.74554713070393	10.9383907918818	34.4199511371553	41.9291915167123	23.290436508134
"""

class GenerateTable(remus.Target):
	def __init__(self, tableName):
		remus.Target.__init__(self)
		self.tableName = tableName
	
	def run(self):
		inTable = self.createTable( self.tableName )
		reader = csv.reader( startText.split("\n"), delimiter="\t" )
		header = None
		for row in reader:
			if len(row):
				if header is None:
					header = {}
					for i, col in enumerate( row[1:] ):
						header[ i ] = col
				else:
					out = {}
					for i, col in enumerate( row[1:] ):
						out[ header[i] ] = float(col)
					inTable.emit( row[0], out )

class TableMap(remus.MapTarget):
	
	def map(self, key, val):
		total = sum(val.values())
		self.emit( key, total / len(val.values()) )
		
		
