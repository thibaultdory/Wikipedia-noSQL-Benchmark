import java.util.ArrayList;

import utils.mathTools;




public class test {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		Cassandra6to12();
		HBase6to12();
		mongoDB6to12();
		Cassandra12to24();
		HBase12to24();
		mongoDB12to24();
		
	}
	
	public static void mongoDB12to24(){
		ArrayList<Double> average = new ArrayList<Double>();
		average.add(81.9695873975);
		average.add(82.4121437311);
		average.add(76.5258684415);
		average.add(73.5834041472);
		average.add(95.7961827604);
		average.add(88.8564545582);
		average.add(90.6178735748);
		average.add(95.3068707258);
		average.add(88.0566546914);
		average.add(81.1536935545);
		average.add(76.8199828231);
		average.add(78.6559002535);
		average.add(62.1631791411);
		average.add(65.178551142);
		average.add(68.0076715525);
		average.add(150.1851053008);
		average.add(85.3739822885);
		average.add(129.4830491061);
		average.add(67.3532177436);
		average.add(67.1284400283);
		average.add(69.3651192429);
		average.add(66.4421463107);
		average.add(77.6427210277);
		average.add(47.1087587353);
		average.add(56.4403375571);
		average.add(55.3595511789);
		average.add(49.9064072768);
		average.add(44.5476553283);
		average.add(38.7816783489);
		average.add(38.3526814679);


		ArrayList<Double> time = new ArrayList<Double>();
		time.add(148.73);
		time.add(162.47);
		time.add(175.22);
		time.add(187.48);
		time.add(203.45);
		time.add(218.26);
		time.add(233.36);
		time.add(249.25);
		time.add(263.92);
		time.add(277.45);
		time.add(290.25);
		time.add(303.36);
		time.add(313.72);
		time.add(324.59);
		time.add(335.92);
		time.add(360.95);
		time.add(375.18);
		time.add(396.76);
		time.add(407.99);
		time.add(419.17);
		time.add(430.73);
		time.add(441.81);
		time.add(454.75);
		time.add(462.60);
		time.add(472.01);
		time.add(481.23);
		time.add(489.55);
		time.add(496.98);
		time.add(503.44);
		time.add(509.83);

		
		ArrayList<Double> sd = new ArrayList<Double>();
		sd.add(3.0170482338);
		sd.add(3.6663041865);
		sd.add(4.3582208017);
		sd.add(6.2644105909);
		sd.add(10.6524867821);
		sd.add(5.7149963203);
		sd.add(5.5358884291);
		sd.add(8.1290721098);							
		sd.add(5.7484238244);		
		sd.add(7.8231242774);
		sd.add(4.5361006765);
		sd.add(6.1053371407	);								
		sd.add(4.816775761);
		sd.add(11.9182264384);
		sd.add(20.3990654374);
		sd.add(16.2875907616);
		sd.add(35.2642636733);
		sd.add(5.1827017406);
		sd.add(18.3112960204);
		sd.add(16.2189905025);
		sd.add(11.861950468);
		sd.add(10.3124029811);
		sd.add(15.751629521);
		sd.add(1.6424371409);
		sd.add(6.4616782184);
		sd.add(6.0443810418);
		sd.add(4.8776356778);
		sd.add(6.5128081939);
		sd.add(2.6179247979);
		sd.add(2.3210865378);

		
		double res = mathTools.elasticityScalar(average, time, sd,12,12);
		System.out.println("Elasticity mongoDB 12 to 24 nodes : "+res);
	}
	
	public static void HBase12to24(){
		ArrayList<Double> average = new ArrayList<Double>();
		average.add(56.5506347602);
		average.add(46.0785590276);
		average.add(45.4189070338);
		average.add(45.6395808088);
		average.add(44.5171542982);
		average.add(44.9867183471);

		ArrayList<Double> time = new ArrayList<Double>();
		time.add(61.11);
		time.add(68.79);
		time.add(76.36);
		time.add(83.96);
		time.add(91.38);
		time.add(98.88);
		
		ArrayList<Double> sd = new ArrayList<Double>();
		sd.add(11.9084703619);
		sd.add(10.4720757326);
		sd.add(0.6596519938);
		sd.add(0.220673775);
		sd.add(1.1224265106);
		sd.add(0.4695640489);
		
		double res = mathTools.elasticityScalar(average, time, sd,12,12);
		System.out.println("Elasticity HBase 12 to 24 nodes : "+res);
	}
	
	public static void mongoDB6to12(){
		ArrayList<Double> average = new ArrayList<Double>();
		average.add(65.265856817);
		average.add(113.1450771154);
		average.add(154.8558684481);
		average.add(104.0593515627);
		average.add(51.3744305926);
		average.add(55.6153685974);
		average.add(67.8266153981);
		average.add(61.6705007733);
		average.add(55.6176466822);
		average.add(48.1412169991);
		average.add(47.728128517);
		average.add(58.6373387517);
		average.add(61.3521305122);
		average.add(46.6270993835);
		average.add(32.7938299854);
		average.add(30.5691420719);
		average.add(28.216801307);
		average.add(31.17480909);
		average.add(32.8400745494);
		average.add(29.3461679089);

		ArrayList<Double> time = new ArrayList<Double>();
		time.add(112.00);
		time.add(130.86);
		time.add(156.67);
		time.add(174.01);
		time.add(182.58);
		time.add(191.85);
		time.add(203.15);
		time.add(213.43);
		time.add(222.70);
		time.add(230.72);
		time.add(238.68);
		time.add(248.45);
		time.add(258.67);
		time.add(266.45);
		time.add(271.91);
		time.add(277.01);
		time.add(281.71);
		time.add(286.90);
		time.add(292.38);
		time.add(297.27);
		
		ArrayList<Double> sd = new ArrayList<Double>();
		sd.add(4.6858779831);
		sd.add(58.9514040933);
		sd.add(46.5333529764);
		sd.add(60.8065625701);
		sd.add(5.8275724881);
		sd.add(8.6538323236);
		sd.add(13.6825604626);
		sd.add(12.064533164);
		sd.add(7.1523099045);
		sd.add(8.2353724029);
		sd.add(8.7493312201);
		sd.add(6.573198881);
		sd.add(8.0084276056);
		sd.add(10.8052798499);
		sd.add(3.4840843719);
		sd.add(5.0108673179);
		sd.add(3.8633925914);
		sd.add(5.8344630843);
		sd.add(7.3187401462);
		sd.add(4.081619106);

		
		double res = mathTools.elasticityScalar(average, time, sd,6,6);
		System.out.println("Elasticity mongoDB 6 to 12 nodes : "+res);
	}
	
	public static void HBase6to12(){
		ArrayList<Double> average = new ArrayList<Double>();
		average.add(36.3619356316);
		average.add(33.1230991404);
		average.add(32.7054468983);
		average.add(33.7445367577);
		average.add(32.9884807168);
		average.add(33.0344117738);
		average.add(32.6717215728);
		average.add(32.9302707562);


		ArrayList<Double> time = new ArrayList<Double>();
		time.add(99.88);
		time.add(105.40);
		time.add(110.85);
		time.add(116.48);
		time.add(121.97);
		time.add(127.48);
		time.add(132.92);
		time.add(138.41);


		
		ArrayList<Double> sd = new ArrayList<Double>();
		sd.add(5.342126814);
		sd.add(0.9354738014);
		sd.add(1.0361008294);
		sd.add(1.3137459979);
		sd.add(1.039613135);
		sd.add(1.1906039273);
		sd.add(1.0161359823);
		sd.add(0.7871113646);


		
		double res = mathTools.elasticityScalar(average, time, sd,6,6);
		System.out.println("Elasticity HBase 6 to 12 nodes : "+res);
	}
	public static void Cassandra12to24(){
		ArrayList<Double> average = new ArrayList<Double>();
		average.add(134.1527563764);
		average.add(130.42026872);
		average.add(123.5872924458);
		average.add(102.0850762245);
		average.add(105.3704467703);
		average.add(103.9291625273);
		average.add(112.0556233222);
		average.add(87.1365473871);
		average.add(89.31697762);
		average.add(88.2714103283);
		average.add(79.0367828152);
		average.add(78.1789340955);
		average.add(78.7535753719);
		average.add(83.4730033897);
		average.add(82.3857704564);

		ArrayList<Double> time = new ArrayList<Double>();
		time.add(271.66);
		time.add(293.39);
		time.add(313.99);
		time.add(331.00);
		time.add(348.57);
		time.add(365.89);
		time.add(384.56);
		time.add(399.09);
		time.add(413.97);
		time.add(428.68);
		time.add(441.86);
		time.add(454.89);
		time.add(468.01);
		time.add(481.92);
		time.add(495.66);
		
		ArrayList<Double> sd = new ArrayList<Double>();
		sd.add(16.5177517111);
		sd.add(7.2898854023);
		sd.add(17.8875752887);
		sd.add(5.4673299367);
		sd.add(6.2607212417);
		sd.add(15.2750244005);
		sd.add(5.8071433344);
		sd.add(2.3903396333);
		sd.add(4.2871160851);
		sd.add(16.4392236861);
		sd.add(9.7568870832);
		sd.add(7.1924973893);
		sd.add(2.1023939314);
		sd.add(6.9051567339);
		sd.add(5.7554365235);

		
		double res = mathTools.elasticityScalar(average, time, sd,12,12);
		System.out.println("Elasticity Cassandra 12 to 24 nodes : "+res);
	}

	public static void Cassandra6to12(){
		ArrayList<Double> average = new ArrayList<Double>();
		average.add(141.4602977074);
		average.add(104.7894377403);
		average.add(93.4582127216);
		average.add(93.8259207625);
		average.add(98.125427651);
		average.add(87.6037037531);
		average.add(79.1616564074);
		average.add(58.771983227);
		average.add(54.2345180532);
		average.add(54.3691743067);

		ArrayList<Double> time = new ArrayList<Double>();
		time.add(189.13);
		time.add(206.59);
		time.add(222.17);
		time.add(237.81);
		time.add(254.16);
		time.add(268.76);
		time.add(281.95);
		time.add(291.75);
		time.add(300.79);
		time.add(309.85);

		
		ArrayList<Double> sd = new ArrayList<Double>();
		sd.add(189.13);
		sd.add(206.59);
		sd.add(222.17);
		sd.add(237.81);
		sd.add(254.16);
		sd.add(268.76);
		sd.add(281.95);
		sd.add(291.75);
		sd.add(300.79);
		sd.add(309.85);

		
		double res = mathTools.elasticityScalar(average, time, sd,6,6);
		System.out.println("Elasticity Cassandra 6 to 12 nodes : "+res);
	}
}
