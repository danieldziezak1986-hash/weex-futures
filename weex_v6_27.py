# =====================================================================================
# WEEX FUTURES V6.19 - JEDEN PLIK
# Colab (bez kopiowania tekstu): w komorce
#   from google.colab import files; exec(open(list(files.upload())[0], encoding='utf-8').read())
# i wybierz ten plik. Lokalnie: python weex_v6.py
# Wynik: weex_v6_RRRRMMDD_HHMMSS.zip -> top_setups.csv (najlepsze MARKET i LIMIT), setups.csv (po filtrach),
#        hist_summary.csv (test historyczny 30 dni),
#        setups_all.csv (z odrzuconymi),
#        summary.csv/json, backtest.csv, backtest_archive.csv, gate.csv, run_meta.json, qc.json,
#        coins/*.json (surowe dane - pozwalaja odtworzyc analize: REPLAY_ZIP)
#
# Co nowego w V6.15 (17.10):
#  - BRAMKA statystyczna osobno dla typu, LONG/SHORT oraz MARKET/LIMIT; min. 60 transakcji, 20 zrzutow i 7 dni.
#  - t-stat przewagi nad kontrola liczony na srednich dziennych (mniej ryzyka pseudoprobki ze zrzutow co 2h).
#  - TOP wymaga statusu DO GRY oraz do_handlu=True po wszystkich twardych filtrach; ocena A/B sama nie wystarcza.
#  - Dane z V6.14 nie potwierdzaja obecnie zadnego setupu do wejscia; nowa bramka pozostawi je w OBSERWACJI.
#
# Co nowego w V6.14 (15.10 wieczor - osobny rynek TRADFI):
#  - WYBOR KONTRAKTOW: TOP_N = tylko krypto (170), TRADFI_N = osobna pula akcji/indeksow/metali/surowcow (60) wg wolumenu.
#    Lacznie ~230 kontraktow (pobieranie ~15% dluzej); wczesniej TradFi zajmowal ~50 miejsc z puli 200
#  - TEST AKCJI/INDEKSOW: test wsteczny 7 dni obejmuje teraz akcje, indeksy i Pre-IPO, ale TYLKO zdjecia w sesji USA
#    (pn-pt 9:30-16:00 NY). Ich wyniki NIE mieszaja sie z krypto: bramka, tryb ochronny, ocena historii i hist_cechy
#    dla krypto licza sie bez akcji (jak dotad). Osobna tabela tradfi_test.csv (typ + kierunek + wejscie)
#  - filtr 'tradfi_bez_testu' zdejmowany dla typu+kierunku+wejscia, ktory w tescie akcji ma >= TRADFI_MIN_N transakcji,
#    sredni wynik > 0, przewage nad losowym wejsciem > 0 i t >= TRADFI_MIN_T. Do tego czasu akcje zostaja z filtrem (D)
#  - TradFi: prog OI 0.1M (MIN_OI_TRADFI_USDT) zamiast 1M; spread, glebokosc booka i wolumen jak dla krypto
#  - TRYB OCHRONNY z 3 dni zamiast 7 (OCHRONA_DNI=3, OCHRONA_MIN_N=20) - test 16.10 w obu polowach 30 dni i archiwum
#  - raport: osobna sekcja TRADFI (MARKET i LIMIT z ocena) + stan testu akcji
#
# Co nowego w V6.13 (15.10 wieczor - dodatkowe dane, TYLKO INFORMACJA / ZAPIS):
#  - ZMIANA OI W TESCIE WSTECZNYM: kazdy setup z testu 7 dni dostaje oi_chg_1h_bt / oi_chg_4h_bt / oi_chg_24h_bt
#    (z historii OI na Drive - oi_history_v63.json, zbieranej przy kazdym uruchomieniu). WEEX nie daje historii OI,
#    wiec kolumny wypelniaja sie tylko tam, gdzie skrypt byl wtedy uruchomiony (im czesciej, tym wiecej danych)
#  - TLUM Z GATE.IO (GATE_STATS): WEEX nie ma w API stosunku long/short ani likwidacji. Skrypt pobiera je z publicznego
#    API Gate.io (futures contract_stats, 1h, ~100h) dla GATE_TOP_N najplynniejszych coinow + coinow z setupow:
#    gate_lsr_konta (konta long/konta short), jego z-score vs ostatnie 48h, gate_lsr_taker, likwidacje long/short 4h (USD),
#    zmiana OI 4h na Gate. To INNA GIELDA - przyblizenie zachowania tlumu, nie dane WEEX.
#    Zapis: gate_stats.csv w zipie (replay go uzywa), gate_stats_history.csv na Drive; w tescie 7 dni te same cechy
#    (bez zagladania w przyszlosc - tylko zamkniete godziny przed zdjeciem). hist_cechy.csv sprawdza nowe cechy
#    (oi_rosnie_4h, oi_spada_4h, tlum_ta_sama_strona, likw_tej_strony) w obu polowach okresu.
#    Nic z tego nie zmienia ocen, filtrow ani bramki. Bledy/brak sieci -> pominiete bez przerywania analizy
#
# Co nowego w V6.12 (15.10 wieczor - alarm odbicia / squeeze, TYLKO INFORMACJA):
#  - SQUEEZE WATCH: gdy rynek jest skrajnie wyprzedany (w ostatnich 24h >= SQUEEZE_BREADTH z top 60 coinow ma RSI 1h < 30)
#    i BTC zebral dolek 24h i zamknal swiece 1h z powrotem nad nim -> raport pokazuje ALARM ODBICIA i 5 kandydatow na long
#    (najsilniejsze plynne alty ze struktura 15m w gore, SL pod ich dolkiem 24h, ilosc dla SQUEEZE_RYZYKO_R).
#    Badanie 15.10 (40 dni 1h, 144 coiny): wyprzedanie >= 40% (okno 24h) + zebranie dolka BTC: 8 zdarzen, koszyk altow
#    +2.34% w 24h (baza +0.75%), BTC +0.68%, 75% wzrostow (okno 6h: 5 zdarzen, +2.96%). Ciasny SL pod dolkiem BTC
#    tracil (-0.3..-0.5R) - dolki sa zbierane po kilka razy. Samo zebranie dolka bez wyprzedania: brak przewagi (-0.27R).
#    ZA MALO ZDARZEN na regule - dlatego tylko alarm + squeeze_log.csv na Drive do oceny po kolejnych tygodniach.
#    Nie zmienia ocen, filtrow ani bramki
#  - OCENA: SHORT na coinie slabszym od BTC o > 2% w 24h -> -1 pkt (OCENA_SHORT_SLABY_RS). Gorsze w 4/4 polowach
#    (archiwum WEEX i test 30 dni) - shortowanie tego, co juz spadlo najmocniej, czesto lapie odbicie (np. ACE, OP 15.10).
#    Tylko ocena A-D, bez zmiany filtrow i bramki
#
# Co nowego w V6.11 (15.10 - realizm testu wstecznego i kontrola dnia):
#  - LIMIT WYPELNIONY DOPIERO PO PRZEBICIU wejscia o LIMIT_PRZEBICIE_BPS (domyslnie 2 bps). V6.10 liczyl wypelnienie
#    przy samym dotknieciu. Analiza 15.10: 3-5% limitow "ledwo dotknietych" (MAE < 0.05R) dawalo srednio +1.3..+1.5R
#    i 1/3 calego zysku testu 30 dni (+203R -> +136R bez nich); w 7 dniach plus zamienial sie w minus (+18R -> -9R).
#    Na gieldzie zlecenie stoi w kolejce - samo dotkniecie czesto go nie wypelnia. Wyniki spadna, ale beda uczciwsze
#  - FUNDING W WYNIKU: kazda stawka fundingu w czasie trzymania pozycji (od wypelnienia do wyjscia) obciaza/zasila
#    wynik (LONG placi dodatnia stawke, SHORT ja dostaje). Kolumna funding_R. Kontrola losowa (control_R) bez fundingu
#  - POSLIZG ZALEZNY OD PARY: SLIP_BPS + polowa spreadu + wplyw zlecenia ORDER_SIZE_USDT na book +-0.5%
#    (max SLIP_MAX_BPS). Kolumna slip_bps. Book z chwili uruchomienia (w tescie wstecznym przyblizenie)
#  - DZIENNY LIMIT STRATY: dziennik.csv na Drive (wzor: dziennik_wzor.csv w zipie). Suma wynikow z dzisiaj (czas PL)
#    <= -DZIENNY_LIMIT_R -> wszystkie setupy 'nie do handlu' ('dzienny limit straty'). Zamiast pliku mozna wpisac
#    WYNIK_DNIA_R. dziennik_podsumowanie.csv: wyniki wg zrodla (skrypt / reczna) i wg typu setupu
#  - podpis cache testu 30 dni obejmuje nowe ustawienia -> pierwsze uruchomienie V6.11 liczy test od nowa (~3-5 min)
#  - archiwum: wiersze z V6.11 maja kolumne metoda_sym='v611'; starsze (powyzej 7 dni) zostaja ze stara metoda
#    i znikaja z archiwum w ciagu BT_ARCHIVE_DAYS
#
# Co nowego w V6.10 (15.10 rano - analiza archiwum WEEX, testu 7 dni i testu 30 dni, setupy po filtrach):
#  - FILTR short_godziny ZAMIAST azja_short: brak SHORT tylko o 02-03 UTC (04-05 PL). Po filtrach SHORT o 02-03 UTC
#    tracily w OBU polowach wszystkich 3 zbiorow: archiwum -0.68/-0.62R, 7 dni -0.77/-0.20R, 30 dni -0.30/-0.92R.
#    Pozostale godziny azjatyckie (00, 04, 06 UTC) ~0R (archiwum +0.02/+0.03R) - stary filtr blokowal je bez powodu.
#    Stary filtr dalej liczony jako wariant w tescie 30 dni (hist_warianty.csv: 'azja_short_stary')
#  - GATE_FROM_UTC przesuniete na 15.10 08:00 UTC (nowa regula dopasowana do danych sprzed tej chwili)
#  - NOWE CECHY zapisywane przy kazdym setupie (na zywo i w tescie): funding_z_hist (funding z w chwili setupu),
#    hour_utc, vol_rank w tescie wstecznym (ranking wolumenu 24h ze swiec 1h w chwili zdjecia), asset_type.
#    Pozwola sprawdzic, czy zatloczenie (funding) i wielkosc pary realnie zmieniaja wynik - na razie bez wplywu na ocene
#  - DANE NIEAKTUALNE: kontrakt z bledem pobierania swiec 5m jest pobierany drugi raz na koncu; gdy dalej blad albo
#    ostatnia swieca 5m jest starsza niz DANE_MAX_WIEK_MIN -> tradable False ('dane nieaktualne'), wpis w qc.json.
#    (V6.9: przy bledzie API analiza cicho uzywala swiec z archiwum - np. ENS 15.10 z RSI sprzed godziny)
#  - sprawdzone i NIE wprowadzone (brak zgodnosci w obu polowach po filtrach): ostrzejsza kara za odlegle LIMIT
#    (<=0.4 ATR lepsze tylko bez filtrow), wykluczenie Commodities (po filtrach za malo transakcji: 22 i 5),
#    ograniczenie do 150 par (pary 151-200 nie gorsze: SHORT +0.30/+0.34R vs +0.22/+0.30R dla top 50)
#
# Co nowego w V6.9 (14.10 wieczor - po pierwszym przebiegu V6.8):
#  - mniej zapytan API (pobieranie bylo na limicie MAX_REQ_PER_SEC: ~1400 zapytan = ~140 s):
#    * 15m/1h/4h: gdy archiwum ma juz ostatnia ZAMKNIETA swiece -> bez zapytania (4h zwykle 3 z 4 godzin, 1h gdy
#      poprzednie uruchomienie bylo w tej samej godzinie). 5m zawsze (cena, swieca w trakcie)
#    * 5m przyrostowo: WEEX nie obsluguje stronicowania, wiec pelne pobranie i tak daje max 1000 swiec -> archiwum
#      wystarczy, gdy pokrywa 1000 swiec (V6.8 wymagal 2600 i 5m bylo zawsze pobierane w calosci)
#    * archiwum 4h trzymane 90 dni (bylo 81 < 500 swiec 4h, wiec 4h zawsze pobierane w calosci)
#    * FUNDING z pamieci na Drive (funding_cache_v69.json): pobierany tylko, gdy od ostatniej stawki minal jej
#      interwal (8h/4h/1h), czyli zwykle raz na kilka uruchomien zamiast 200 zapytan za kazdym razem
#    * licznik zapytan API w raporcie i run_meta.json (zapytania_api)
#  - test historyczny liczy sie od nowa po zmianie fundingu (V6.8 nie mial tego w podpisie cache -> CROWD wciaz
#    nietestowany). Pierwsze uruchomienie V6.9: test 30 dni od nowa (~3-5 min dluzej, raz)
#  - TOP bez setupow z ocena D (V6.8 pokazywal w TOP setupy D, bo TOP patrzy tylko na historie typu)
#  - w_limicie_ryzyka zawsze True/False (bylo puste dla setupow spoza rankingu)
#  - blad zapisu archiwum swiec na Drive nie przerywa sklejania danych (tylko ostrzezenie)
#
# Co nowego w V6.8 (14.10 - szybkosc i dokladnosc):
#  - SWIECE PRZYROSTOWO (INCREMENTAL_KLINES): gdy archiwum swiec na Drive jest aktualne, pobierane sa tylko nowe
#    swiece (5m: 1 zapytanie ~50-100 swiec zamiast 3 x 1000). Brak/stare archiwum -> pelne pobieranie jak dotad.
#    coins/*.json w zipie maja dalej pelne okno swiec (z archiwum + nowe), wiec REPLAY dziala jak wczesniej
#  - premiumIndex dla wszystkich kontraktow jednym zapytaniem (gdy API to obsluguje; inaczej jak dotad po 1 na kontrakt)
#  - zapis JSON ~10x szybszy (json.dumps), zip z kompresja 1 (szybciej, plik nieco wiekszy)
#  - CZASY ETAPOW w raporcie i run_meta.json (czasy_s) - widac, co trwa najdluzej
#  - FUNDING: historia 200 stawek zamiast 30 (fallback 30). Wczesniej test 30 dni mial funding tylko z ~10 ostatnich
#    dni, a przy swiecach spoza WEEX wcale -> CROWD prawie nie byl testowany (brak w hist_summary). funding z na zywo
#    dalej z 30 ostatnich stawek (bez zmiany oceny)
#  - test historyczny ze swiec spoza WEEX dostaje historie fundingu z WEEX (CROWD testowany na calym okresie)
#  - QC: luki w swiecach (brakujace swiece w oknie analizy) -> qc.json 'luki_swiec'
#
# Co nowego w V6.7 (14.10, analiza najwiekszych wzrostow/spadkow 24h):
#  - CECHY RUCHU przy kazdym setupie (tylko zapis i test, ocena bez zmian - OCENA_CECHY_RUCHU = False):
#      ruch24_kier = ranking zmiany 24h kontraktu W STRONE transakcji (0-100; 100 = najmocniej w strone setupu),
#      kontra_24h  = setup przeciw ruchowi 24h (SHORT na top 20% wzrostow / LONG na top 20% spadkow),
#      pogon_24h   = setup z ruchem (top 20% w strone transakcji), wolumen_skok = vol_ratio_1h >= 1.8.
#    Test 30 dni (14.10): kontra +0.45/+0.33R (przewaga +0.26/+0.33) vs pogon +0.17/-0.11R; wolumen_skok przewaga
#    +0.29/+0.18 vs +0.07/+0.09 bez skoku. Znalezione po fakcie -> hist_cechy.csv sprawdza je przy kazdym uruchomieniu
#    (test 30 dni + archiwum WEEX); punkty do oceny dopiero, gdy wynik sie utrzyma na nowych danych
#  - OI: WEEX podaje dla czesci kontraktow zawyzone OI (np. BOME 4.2 mld USDT przy 1.9 mln wolumenu, STRK > podaz).
#    OI > OI_MAX_DO_WOLUMENU x wolumen 24h -> oi_usdt puste, oi_wiarygodne False, wpis w qc.json, bez filtra OI
#    (kontrakt dalej musi przejsc filtr wolumenu i booka). Zmiany OI w % liczone dalej (te same jednostki)
#
# Co nowego w V6.6 (14.10, po pierwszym przebiegu V6.5):
#  - w_limicie_ryzyka pomija strone w trybie ochronnym (V6.5 wybieral np. VELVET LONG przy ochronie LONG)
#  - HIST_WARIANTY: zostaje tylko koszt_max_0.15. Wyniki 30 dni z 14.10 18:37:
#    CONT SHORT 0.05 ATR (kontrola) gorszy w obu polowach -> 0.15 zostaje; 'tylko swieze FVG' i 'CONT bez retestu'
#    gorsze; CONT LONG 0.15 ATR mieszany. Mniej wariantow = test historyczny ~8 min krocej
#  - MAX_COST_R bez zmian (0.10): 0.15 dla wszystkich lepszy w obu polowach tylko lacznie (+14.8/+6.5R), ale typy
#    odwracaja sie miedzy polowami (PULLBACK +16.9/-26.1R, CONT -2.3/+35.5R); 0.15 tylko dla CONT nie przechodzi
#    (1. polowa -2.3R). Wariant zostaje w tescie - zmiana, gdy wynik sie utrzyma
#
# Co nowego w V6.5 (14.10 wieczor):
#  - CENA NA ZYWO: ticker byl pobierany ~6 min przed swiecami (u niektorych roznica 0.5-0.9%). Teraz na koncu
#    pobierania ticker jest pobierany drugi raz (ticker_live) i to on jest cena setupow, wejsc MARKET i alarmu BTC.
#    Stare zipy (replay): cena = zamkniecie biezacej swiecy 5m, potem ticker
#  - CONT_SWEEP.v4: SHORT z LIMIT 0.15 ATR przed poziomem (CONT_ENTRY_FRONT_ATR_SHORT). Wariant z testu 30 dni:
#    SHORT +10.3R, lepszy w obu polowach (+2.9R / +7.4R), sr. 0.22 -> 0.26R. LONG bez zmian (0.05 ATR, wynik mieszany)
#  - CONT_SWEEP: kolumna retesty_15m (ile swiec 15m po wybiciu juz dotknelo poziomu wejscia). Replay 7 dni: brak
#    roznicy -> tylko informacja; filtr CONT_MAX_RETESTY sprawdzany jako wariant w tescie 30 dni
#  - SWING4H wylaczony (WYLACZONE_TYPY, LONG i SHORT): po filtrach tracil w obu polowach 30 dni (LIMIT LONG
#    -0.01/-0.32R, SHORT -0.41/-0.01R). Dalej liczony w tescie jako 'typ_wylaczony' - mozna wrocic, gdy sie poprawi
#  - EKSPOZYCJA: ten sam kontrakt i kierunek (MARKET + LIMIT) liczony raz; kolumna w_limicie_ryzyka = najlepsze
#    setupy A/B, ktore mieszcza sie w MAX_RYZYKO_KIERUNEK_PCT razem z otwartymi pozycjami
#  - LIMIT: wazne_do_utc / wazne_do_pl (godzina anulowania) zamiast samego 'po Nh'
#  - TOP: bez setupow ze strony w trybie ochronnym (wczesniej TOP pokazywal np. LONG, gdy LONG byl 'nie do handlu')
#  - ocena: zrodlo historii w opisie (bylo zawsze 'historia 30d', nawet gdy liczona z archiwum 7 dni)
#  - HIST_WARIANTY: MAX_COST_R 0.15, tylko swieze FVG (fill 0%), CONT bez retestu, CONT LONG 0.15 ATR, kontrola SHORT 0.05
#  - podpis cache testu historycznego obejmuje teraz takze WYLACZONE_TYPY i LIMIT_MAX_FILL_PCT (wczesniej zmiana
#    tych ustawien zostawiala w cache stare filtry)
#
# Co nowego w V6.4 (14.10, analiza testu 30 dni + archiwum 7 dni):
#  - FILTR azja_short: brak SHORT w sesji azjatyckiej 00-07 UTC (tracily w obu probach: -0.25R i -0.52R/tr.
#    po filtrach, CONT_SWEEP SHORT poza Azja +0.23R i +0.98R). Odrzucone dalej testowane: "odrzucone: azja_short"
#  - FILTR fvg4h: SWING4H tylko z FVG 1h (wersja z FVG 4h: -0.28R w tescie 30 dni)
#  - opcje TRAIL_PO_1R_R / HOLD_PO_1R_H (trailing i dluzsze trzymanie po 1R) - w tescie gorzej/bez roznicy -> wylaczone
#  - GATE_FROM_UTC przesuniete na 14.10 16:00 UTC: nowe filtry dopasowano do danych sprzed tej chwili
#  - status TRAIL w wynikach (tylko gdy TRAIL_PO_1R_R wlaczony)
#  - TRYB OCHRONNY PER STRONA (OCHRONA_PER_STRONA): LONG i SHORT liczone osobno - strona z ujemnym wynikiem 7 dni
#    nie do handlu (ryzyko x0.5), druga bez ograniczen. Tam bylo najwiecej SL (40-64% vs 32-39%)
#  - opcja SL_ZA_KLASTER (SL za klaster likwidacji) i test szerszych SL (bufor 0.25/0.4 ATR, min SL 0.7 ATR, CONT 1.2 ATR):
#    odsetek SL bez zmian (~41%), wynik gorszy albo bez roznicy -> SL bez zmian
#  - BADANIE RYNKU (kierunek.csv, przegrzanie.csv): przy kazdym uruchomieniu test sygnalow kierunku (trend BTC 1h/4h,
#    szerokosc rynku, momentum koszyka) i przegrzania (RSI, wychylenie od EMA50, ruch 24h) na swiecach 1h z archiwum,
#    osobno w obu polowach historii. Regula wlacza sie sama tylko, gdy obie polowy zgodne (t >= BADANIE_MIN_T).
#    Test 14.10 (41 dni 1h, 83 dni 4h): zaden sygnal kierunku nie przeszedl; gra PRZECIW przegrzanemu wzrostowi tracila
#    w obu polowach (ruch zwykle trwa dalej) -> przy takim wyniku SHORT na przegrzanym wzroscie = nie do handlu
#  - OCENA: zgodnosc momentum (VWAP dzienny, RSI 15m, zmiana 4h w strone transakcji); <= 1 z 3 -> -1 pkt (OCENA_MOMENTUM).
#    Sprawdzone 35 cech zapisywanych przy setupach: tylko koszt/odleglosc SL i momentum w strone transakcji dzialaly w obu probach
#  - WARIANTY USTAWIEN (HIST_WARIANTY -> hist_warianty.csv): test historyczny 30 dni liczony tez z innymi ustawieniami
#    (np. LIMIT na krawedzi FVG, CONT_SWEEP dalej przed poziomem) na tych samych swiecach; porownanie w obu polowach
#  - FVG_WEJSCIE_POZ = 1.0: LIMIT na krawedzi FVG blizszej cenie zamiast CE (wygral w obu polowach 30 dni: +42.7R).
#    Nowe wersje typow z wejsciem w FVG: REV_MSS.v4, PULLBACK.v4, SWING4H.v2, CROWD.v2 (statystyka i bramka od nowa)
#  - 'do handlu' wymaga teraz takze oceny A-C, braku filtra i tradable (wczesniej mogl byc przy D / odrzuconych)
#
# Co nowego w V6.3 (na podstawie analizy 210 transakcji z V6.1 i 40 dni swiec 1h):
#  - prowizje z danych kontraktu (WEEX: taker 0.08%, maker 0.02-0.08%), nie stale 6/2 bps
#  - FILTRY: koszt transakcji <= MAX_COST_R ryzyka; brak wejsc "w pogoni" (cena wejscia daleko od EMA50 1h
#    w strone transakcji); brak SHORT na mocno wyciagnietych w gore kontraktach (squeeze)
#  - wejscia LIMIT oraz MARKET (MARKET tylko gdy cena JUZ jest w strefie wejscia - bez gonienia ceny);
#    kazde ma osobny wynik w tescie i osobna bramke (typ.vN = LIMIT, typ.vN.M = MARKET)
#  - POROWNANIE Z LOSOWYM WEJSCIEM: kazda transakcja testu ma kontrole (CONTROL_K losowych kontraktow, ten sam
#    kierunek, to samo zdjecie, ten sam SL/TP w ATR). edge_R = wynik - kontrola -> oddziela setup od kierunku rynku
#  - TEST HISTORYCZNY: definicje setupow na HIST_DAYS dniach swiec, prowizje WEEX, liczony raz na dobe (cache na Drive)
#    -> hist_summary.csv / hist_backtest.csv. Zrodlo swiec po kolei: wlasne archiwum swiec WEEX (gdy juz obejmuje
#    HIST_DAYS), WEEX historyKlines, Bybit, Binance spot
#  - ARCHIWUM SWIEC WEEX na Drive (candles_v63/): kazde uruchomienie dopisuje swiece 5m/15m/1h/4h -> dluzszy test
#    wsteczny na danych WEEX i po ~30 dniach test historyczny w calosci na WEEX
#  - TOP: najlepsze setupy osobno dla MARKET i LIMIT - ocena = srednia PRZEWAGA (edge_R) tego samego typu, kierunku
#    i rodzaju wejscia (z testu historycznego, inaczej z archiwum), sciagnieta do zera przy malej probie
#  - bramka wymaga teraz takze dodatniej przewagi: sredni edge >= GATE_MIN_EDGE_R i t(edge) >= GATE_MIN_T
#  - 3 typy setupow z wlasnymi regulami (wersje definicji w SETUP_TYPES):
#    REV_MSS    sweep plynnosci (poziom 1h) na 15m -> CHoCH 5m po ekstremum -> LIMIT w CE FVG z przemieszczenia
#    CONT_SWEEP wybicie poziomu plynnosci zaakceptowane zamknieciami 15m -> LIMIT na retescie poziomu
#    PULLBACK   struktura i EMA 1h zgodne, 4h i BTC nie przeciw -> LIMIT w CE FVG 15m/1h w biezacej nodze
#  - cechy przy wejsciu zapisywane w setups/backtest (wychylenie od EMA50, RSI, ADX/CHOP, zmiana 24h,
#    sila vs BTC, ranking 24h w przekroju rynku, wolumen, taker buy, VWAP dzienny, sesja, koszt w R...)
#    -> analiza_v6.py pokaze, ktore z nich naprawde odrozniaja wygrane od przegranych
#  - test wsteczny: te same funkcje co na zywo, tylko na swiecach zamknietych przed "zdjeciem" rynku;
#    do ~7 dni wstecz (stronicowanie swiec 5m), MFE/MAE, odrzucone setupy tez symulowane (ocena filtrow)
#  - BRAMKA: typ jest "DO GRY" dopiero, gdy na danych od GATE_FROM_UTC (poza okresem projektowania) ma
#    >= GATE_MIN_TRADES transakcji, sredni wynik >= GATE_MIN_AVG_R i t >= GATE_MIN_T (t po zdjeciach rynku).
#    Wczesniej jego setupy sa "OBSERWACJA". Archiwum wynikow rosnie na Google Drive z kazdym uruchomieniem.
#  - DZWIGNIA (dopisane): przy kazdym setupie plan pozycji - wielkosc z ryzyka (KAPITAL_USDT x RYZYKO_NA_TRANSAKCJE_PCT),
#    dzwignia preferowana / bezpieczna / do uzycia, cena likwidacji (isolated), depozyt, ilosc, strata przy SL.
#    Dzwignia NIE zmienia wyniku w R ani straty przy SL (to robi wielkosc pozycji) - zmienia tylko depozyt
#    i odleglosc likwidacji. Bezpieczna = likwidacja co najmniej LEV_LIQ_SL_MULT razy dalej niz SL.
#  - OTWARTE POZYCJE (dopisane): wpisz je w OTWARTE_POZYCJE albo w otwarte_pozycje.csv na Drive -> pozycje.csv
#    z zaleceniem: TRZYMAJ / PRZESUN_SL / ZAMKNIJ_CZESC / ZAMKNIJ i powodami (struktura 15m/1h po wejsciu,
#    plynnosc w strone pozycji, oddany zysk, czas, przeciwny setup, likwidacja vs SL). SL tylko zaciesniany.
#    Reguly zarzadzania NIE sa jeszcze sprawdzone w tescie wstecznym - to porzadek decyzji, nie udowodniona przewaga.
#  - POZYCJE Z KONTA (dopisane): z kluczem API WEEX (TYLKO ODCZYT) kolektor sam pobiera otwarte pozycje
#    (/capi/v3/account/position/allPosition) i zlecenia SL/TP (/capi/v3/openAlgoOrders). Klucz w sekretach Colab
#    (ikona kluczyka po lewej): WEEX_API_KEY, WEEX_API_SECRET, WEEX_API_PASSPHRASE. Kod wysyla wylacznie zapytania GET.
#  - OCENA v2 (14.10): punkty strukturalne (rezim 1h, trend 1h BTC, R:R, koszt, trend 4h, wielkosc pary); historia typu
#    tylko informacyjnie; PULLBACK SHORT i REV_MSS SHORT wylaczone; filtr 'poscig' wylaczony; bez kary za wysokie RSI.
#  - OCENA A-D (dopisane): kazdy setup (takze odrzucony filtrem) dostaje punkty: historia typu (t przewagi i sredni R),
#    R:R, koszt w R, zgodnosc z krotkim kierunkiem BTC (struktura 5m), przeciw trendowi 4h, przegrzanie (RSI/funding).
#    ranking.csv + sekcja RANKING w raporcie: osobno MARKET i LIMIT. Ocena to ranking, nie potwierdzona przewaga.
# To narzedzie analityczne, nie porada inwestycyjna. Dzwignia na perpetualach moze zabrac cala pozycje.
# V6.15: bramka osobno dla typu + LONG/SHORT + wejscia (MARKET ma osobna wersje .M); min. 60 transakcji,
# 20 zrzutow i 7 dni, dodatni wynik po kosztach oraz dodatni edge. Istotnosc edge liczona po dniach, nie po zrzutach.
# TOP wymaga dodatkowo przejscia wszystkich warunkow handlu (trend, ochrona, ocena, ryzyko).
# =====================================================================================

# ==================================== USTAWIENIA ====================================
TOP_N = 170                     # V6.14: ile kontraktow KRYPTO (wg wolumenu 24h); ponizej MIN_QUOTE_VOL_USDT nie sa pobierane
TRADFI_ON = True                # V6.14: osobna pula TradFi (akcje, indeksy, Pre-IPO, metale, surowce)
TRADFI_N = 60                   # ...ile kontraktow TradFi wg wolumenu (15.10: 30 -> 60 na prosbe; cienkie i tak 'nie do handlu')
TRADFI_MIN_N = 20               # ...filtr 'tradfi_bez_testu' zdejmowany od tylu transakcji typu+kierunku+wejscia w tescie akcji
TRADFI_MIN_T = 1.0              # ...i przy t przewagi >= tyle (oraz sredni wynik > 0, przewaga > 0)
MALE_PARY_RANK = 100            # pary spoza top N wg wolumenu: w ocenie -1 pkt (plytszy book, wiecej przypadkowych ruchow)
ALWAYS_INCLUDE = ['BTCUSDT', 'ETHUSDT']
TAKER_FEE_BPS = 8.0             # zapasowo, gdy brak w danych kontraktu (WEEX: 0.08%)
MAKER_FEE_BPS = 2.0             # zapasowo (WEEX: 0.02% dla wiekszosci kontraktow)
ORDER_SIZE_USDT = 1000          # wielkosc zlecenia do szacowania poslizgu
SETUP_MIN_RR = 1.5              # min. R:R netto do TP1
MAX_SPREAD_BPS = 10             # tradable: max spread
MIN_DEPTH_USDT = 25_000         # tradable: min. glebokosc booka +-0.5%
MIN_QUOTE_VOL_USDT = 500_000    # tradable: min. wolumen 24h (bylo 3M - na WEEX tylko ~100 par ma >= 3M)
MIN_OI_USDT = 1_000_000         # tradable: min. open interest
MIN_OI_TRADFI_USDT = 100_000    # V6.14: prog OI dla TradFi (akcje maja na WEEX male OI); spread, glebokosc i wolumen bez zmian
LIMIT_MAX_DIST_ATR = 2.0        # max odleglosc zlecenia LIMIT od ceny, w ATR 1h
# --- filtry (V6.3) - setup, ktory ich nie przejdzie, trafia tylko do setups_all.csv i jest testowany osobno
MAX_COST_R = 0.10               # prowizje (wejscie+wyjscie przez SL) / ryzyko; ciasne SL przegrywaly na kosztach
EXT_CHASE_MAX_ATR = 1.5         # max odleglosc ceny wejscia od EMA50 1h W STRONE transakcji (ATR 1h) - bez pogoni
SHORT_SQUEEZE_EXT_ATR = 3.0     # brak SHORT, gdy cena jest wyzej niz EMA50 1h + tyle ATR 1h
# --- wejscia po rynku i ranking
MARKET_ENTRIES = True           # wariant MARKET: wejscie od razu, gdy cena JUZ jest w strefie wejscia setupu
MARKET_ZONE_ATR = 0.25          # ...albo najwyzej tyle ATR 1h przed strefa (po stronie, z ktorej cena nadchodzi)
TOP_N_PER_MODE = 5              # ile najlepszych setupow pokazac osobno dla MARKET i LIMIT
TOP_MIN_T = 2.0                 # do TOP tylko typ+kierunek+wejscie z przewaga nad losowym wejsciem o t >= tyle
TOP_MAX_PER_SIDE = 3            # TOP: najwyzej tyle setupow w jednym kierunku (alty chodza razem z BTC)
FILTER_COUNTER_TREND = True     # filtr: setup przeciw trendowi 4h albo przeciw BTC 1h (przy korelacji >= BTC_CORR_MIN)
LIMIT_MAX_PROGRESS = 0.5        # LIMIT nieaktualny, gdy cena przeszla >= tyle drogi od wejscia do TP1
SLIP_BPS = 3.0                  # poslizg (bps) na kazdym wyjsciu przez SL/timeout i wejsciu po rynku (baza)
SLIP_DYNAMIC = True             # V6.11: poslizg wg pary = SLIP_BPS + spread/2 + 50 bps x ORDER_SIZE_USDT / book +-0.5%
SLIP_MAX_BPS = 30.0             # ...najwyzej tyle
LIMIT_PRZEBICIE_BPS = 2.0       # V6.11: LIMIT wypelniony dopiero, gdy cena przejdzie przez wejscie o tyle bps (0 = dotkniecie)
FUNDING_W_WYNIKU = True         # V6.11: stawki fundingu w czasie trzymania pozycji wliczone w wynik testu (funding_R)
RANK_DAYS = 7                   # ranking: wyniki typu+kierunku+wejscia z archiwum z ostatnich N dni
RANK_SHRINK_K = 30              # sciaganie sredniej do zera: wynik * n / (n + K) - mala proba = slaby sygnal
USE_DRIVE = True                # archiwum testu i historia OI na Google Drive
BACKTEST = True                 # test wsteczny setupow na pobranych swiecach
BT_LOOKBACK_H = 168             # ile godzin wstecz siega test (ogranicza go historia 5m)
BT_STEP_H = 2                   # co ile godzin "zdjecie" rynku w tescie
BT_MAX_HOLD_H = 12              # max czas trzymania pozycji w tescie
BT_ARCHIVE_DAYS = 90            # ile dni trzymac w archiwum
HISTORY_5M_BARS = 2600          # ile swiec 5m pobrac (po 1000 na zapytanie; ~9 dni)
# --- typy setupow i bramka skutecznosci
SETUP_TYPES = {'REV_MSS': 4, 'CONT_SWEEP': 4, 'PULLBACK': 4, 'SWING4H': 2, 'CROWD': 2}   # typ: wersja definicji (zmiana = nowa statystyka)
                                # V6.4: typy z wejsciem w FVG -> nowa wersja (wejscie na krawedzi FVG)
                                # V6.5: CONT_SWEEP.v4 (SHORT 0.15 ATR przed poziomem)
SETUP_WAIT_H = {'REV_MSS': 2, 'CONT_SWEEP': 3, 'PULLBACK': 4, 'SWING4H': 24, 'CROWD': 3}
SETUP_HOLD_H = {'SWING4H': 72}  # max czas trzymania wg typu (reszta: BT_MAX_HOLD_H)  # waznosc zlecenia LIMIT
GATE_FROM_UTC = '2026-10-15T08:00'  # V6.10: nowy filtr short_godziny. Wczesniej 2026-10-14T16:00 - do bramki licza sie tylko zdjecia rynku od tej chwili (V6.4: reguly z 14.10 dopasowane do danych do 15:18 UTC)
GATE_WINDOW_DAYS = 14           # bramka liczona z ostatnich N dni
GATE_MIN_TRADES = 60            # V6.15: min. rozstrzygnietych transakcji dla typu + kierunku + wejscia
GATE_MIN_AVG_R = 0.05           # min. sredni wynik na transakcje (R, po kosztach)
GATE_MIN_T = 2.0                # V6.15: wymagana silniejsza przewaga (t-stat edge)
GATE_MIN_EDGE_R = 0.05          # min. srednia przewaga nad losowym wejsciem (R)
GATE_MIN_SNAPSHOTS = 20         # V6.15: min. liczba zrzutow rynku; nie traktujemy ich jako niezaleznych obserwacji
GATE_MIN_DAYS = 7               # V6.15: edge musi potwierdzic sie w co najmniej 7 roznych dniach
# --- porownanie z losowym wejsciem (oddziela jakosc setupu od kierunku rynku)
CONTROL_K = 8                   # ile losowych kontraktow na setup: to samo zdjecie, ten sam kierunek, ten sam SL/TP w ATR
# --- dlugi test historyczny na swiecach z innej gieldy (WEEX nie daje starszych swiec 5m)
HIST_BACKTEST = True            # raz na HIST_REFRESH_H godzin: test definicji na HIST_DAYS dniach (wynik w cache na Drive)
HIST_SOURCE = 'auto'            # 'auto' = po kolei: archiwum swiec WEEX (gdy obejmuje HIST_DAYS) -> WEEX historyKlines
                                #  -> Bybit (perpetuale) -> Binance spot; albo na sztywno: 'weex_archive','weex','bybit','binance'
SAVE_CANDLES = True             # wlasne archiwum swiec WEEX na Drive (dopisywane przy kazdym uruchomieniu)
# --- szacowana mapa likwidacji (model: wejscia po typowej cenie swiec 1h z LIQ_HOURS, dzwignie LIQ_LEVERAGES)
#     uzycie: ALTERNATYWNY cel TP (tp_liq) - testowany obok zwyklego TP1; nie wplywa na kierunek ani na wybor setupu
LIQ_LEVERAGES = ((25, 0.3), (50, 0.4), (100, 0.3))
LIQ_MMR = 0.005
LIQ_HOURS = 72
LIQ_BIN_PCT = 0.25              # szerokosc kubelka ceny (% ceny) przy grupowaniu likwidacji w klastry
LIQ_MIN_SHARE = 8.0             # klaster = kubelek z >= tyle % szacowanej plynnosci po swojej stronie ceny
LIQ_RANGE_PCT = 15.0            # brane pod uwage klastry najwyzej tyle % od ceny
CANDLE_KEEP_DAYS = 45           # ile dni swiec trzymac w archiwum
HIST_DAYS = 30                  # ile dni historii
HIST_COINS = 50                 # ile kontraktow (najwiekszy wolumen na WEEX; tylko krypto)
HIST_REFRESH_H = 24             # co ile godzin liczyc test od nowa
HIST_WARIANTY = {               # V6.4: te same 30 dni policzone z innymi ustawieniami -> hist_warianty.csv (A/B bez zgadywania).
    'koszt_max_0.15': {'MAX_COST_R': 0.15},                    # 14.10: lacznie lepszy w obu polowach, ale typy niezgodne
    'azja_short_stary': {'FILTR_AZJA_SHORT': True, 'FILTR_SHORT_GODZINY_UTC': ()},   # V6.10: porownanie ze starym filtrem
}                               # 17.10 odrzucone: trail_2R_1R (-0.1R / 1934 tr.), trail_schodki (+0.9R) - p1 gorzej, p2 lepiej = brak efektu                               # odrzucone 14.10: FVG_tylko_swiezy, CONT_bez_retestu, CONT_LONG_0.15ATR, CONT_SHORT_0.05                               # kazdy wariant ~ +2-3 min raz na HIST_REFRESH_H; {} = wylaczone. Zmiana domyslnych dopiero,
                                # gdy wariant jest lepszy w OBU polowach 30 dni (kolumny p1/p2)
# --- dzwignia i wielkosc pozycji (margin ISOLATED) ---
KAPITAL_USDT = 190.0            # 16.10 20:21: stan konta (po wplacie 100) (aktualizuj co kilka dni). 1R = 1% = ~1 USDT
RYZYKO_NA_TRANSAKCJE_PCT = 1.0  # ile % kapitalu tracisz, gdy wejdzie SL (z prowizjami i poslizgiem)
PREFEROWANA_DZWIGNIA = 10       # Twoja dzwignia; obnizana, gdy likwidacja bylaby za blisko SL
LEV_LIQ_SL_MULT = 2.0           # likwidacja min. tyle razy dalej od wejscia niz SL
LEV_MMR = 0.005                 # szacowany maintenance margin (WEEX zalezy od progu pozycji - sprawdz w aplikacji)
MAX_DEPOZYT_PCT = 30.0          # depozyt jednej pozycji najwyzej tyle % kapitalu -> wymusza minimalna dzwignie
# --- otwarte pozycje: wpisz tutaj ALBO w pliku otwarte_pozycje.csv w DRIVE_DIR (te same kolumny; plik ma pierwszenstwo)
# symbol, side (LONG/SHORT), entry, sl (obecny SL), tp (opcjonalnie), sl_start (SL z otwarcia - do liczenia R,
# opcjonalnie), otwarta_utc ('RRRR-MM-DDTHH:MM' UTC, opcjonalnie), dzwignia (opcjonalnie), ilosc (opcjonalnie)
OTWARTE_POZYCJE = [
    # {'symbol': 'TIAUSDT', 'side': 'SHORT', 'entry': 0.5600, 'sl': 0.5804, 'tp': 0.5050,
    #  'otwarta_utc': '2026-10-14T08:30', 'dzwignia': 10, 'ilosc': 500},
]
POZYCJE_CSV = 'otwarte_pozycje.csv'
POZYCJE_Z_KONTA = True          # pobierz otwarte pozycje i SL/TP z konta WEEX (wymaga klucza API tylko do odczytu)
POZYCJE_STAN = 'pozycje_stan.json'   # na Drive: pierwszy widziany SL kazdej pozycji (do liczenia R)
POZ_BE_R = 1.0                  # od tylu R zysku: SL na breakeven (+prowizje)
POZ_TRAIL_R = 0.5               # od tylu R zysku: SL za ostatnim swingiem 15m po wejsciu
POZ_PARTIAL_R = 2.0             # od tylu R zysku: zamknij czesc
POZ_PARTIAL_PCT = 50            # ile % zamknac przy ZAMKNIJ_CZESC
POZ_GIVEBACK = 0.5              # zamknij czesc, gdy po MFE >= 1.5R cena oddala tyle zysku
POZ_TP_NEAR_ATR = 0.3           # "blisko TP" = tyle ATR 1h
# --- ocena setupow A-D ---
OCENA_PROGI = {'A': 2, 'B': 1, 'C': 0}   # punkty strukturalne: >=2 A, 1 B, 0 C, mniej = D (pomin)
OCENA_WAGA_HISTORII = 0         # punkty za historie typu (0 = tylko informacja). Test 14.10: historia 30d nie przewidywala
                                # kolejnych 15 dni (przewaga PULLBACK LONG pochodzila z jednego wzrostowego tygodnia)
WYLACZONE_TYPY = {('PULLBACK', 'SHORT'), ('REV_MSS', 'SHORT'),   # tracily w obu polowach 30 dni -> filtr typ_wylaczony
                  ('SWING4H', 'LONG'), ('SWING4H', 'SHORT')}       # V6.5: SWING4H po filtrach ujemny w obu polowach 30 dni
ZARZADZANIE_1R = True           # 50% pozycji zamykane przy +1R, reszta z SL na wejsciu (breakeven); test 14.10: lepiej w obu polowach 30 dni
CZESC_1R = 0.5                  # jaka czesc zamknac przy 1R
TRAIL_PO_1R_R = None            # opcja: po 1R reszta z trailing stopem (SL = najlepsza cena - tyle R, nie gorzej niz wejscie).
                                # Test 14.10 (replay, 1312 tr.): 1.0R -0.04R/tr., 0.5R -0.09R/tr. vs SL na wejscie -> wylaczone
TRAIL_STOPNIE = None            # V6.14 (test): po 1R stop schodkowo, np. ((2, 1), (3, 2)) = po +2R SL na +1R, po +3R na +2R.
                                # None = jak dotad (po 1R SL na wejsciu). Wlaczyc dopiero po wyniku w hist_warianty.csv (obie polowy)
HOLD_PO_1R_H = None             # opcja: po 1R trzymaj do tylu godzin (inaczej BT_MAX_HOLD_H). Test 14.10: 24h = bez roznicy (+0.004R)
FILTR_AZJA_SHORT = False        # V6.4: brak SHORT w sesji azjatyckiej (00-07 UTC). V6.10: wylaczony, zastapiony przez FILTR_SHORT_GODZINY_UTC
FILTR_SHORT_GODZINY_UTC = (2, 3)   # V6.10: brak SHORT o tych godzinach UTC (02-03 UTC = 04-05 PL); () = wylaczone
DANE_MAX_WIEK_MIN = 15          # V6.10: ostatnia zamknieta swieca 5m starsza niz tyle minut -> 'dane nieaktualne' (tradable False)
SL_ZA_KLASTER = False           # opcja: klaster likwidacji tuz za SL (<= 0.5 ATR) -> SL przesuniety za klaster
SL_KLASTER_BUFOR_ATR = 0.1      # ...o tyle ATR za klaster
SWING4H_BEZ_FVG4H = True        # V6.4: SWING4H tylko z FVG 1h; wersja z FVG 4h tracila (-0.28R w tescie 30 dni)
OCENA_MAX_DIST_ATR = 0.7        # LIMIT dalej od ceny niz tyle ATR 1h -> -1 pkt (gorzej w obu polowach 30 dni)
ALARM_BTC_ATR15 = 1.5           # alarm: BTC przeszedl wiecej niz tyle ATR 15m w ostatnich 30 min
ALARM_BTC_MIN = 30
OCHRONA_DNI = 3                 # tryb ochronny: sredni wynik setupow (po filtrach) z archiwum z ostatnich N dni < 0
                                # 16.10: 7 -> 3 dni. Test (blokada vs reszta, obie polowy): 30 dni 7d -13R/+26R, 3d +22R/+6R;
                                # archiwum WEEX bez zmian (+21R/+8R). 7 dni trzymalo blokade tydzien po jednym zlym dniu (12.10)
OCHRONA_MIN_N = 20              # ...liczony dopiero od tylu rozstrzygnietych transakcji (16.10: 30 -> 20, jak w tescie okna 3 dni)
OCHRONA_MNOZNIK = 0.5           # w trybie ochronnym ryzyko na transakcje x tyle
OCHRONA_PER_STRONA = True       # V6.4: tryb ochronny osobno dla LONG i SHORT: strona z ujemnym wynikiem 7 dni -> nie do handlu
                                # i ryzyko x OCHRONA_MNOZNIK; druga strona bez ograniczen. Test 14.10 (transakcje po filtrach):
                                # 30 dni: strona ujemna -> nastepne -0.03R (SL 40%), dodatnia -> +0.20R (SL 32%);
                                # archiwum 7 dni: ujemna -> -0.42R (SL 64%), dodatnia -> +0.29R (SL 39%)
SQUEEZE_WATCH = True            # V6.12: alarm 'mozliwe odbicie / squeeze' (tylko informacja + log do oceny, NIE zmienia ocen ani filtrow)
SQUEEZE_BREADTH = 0.40          # ...gdy w ostatnich SQUEEZE_OKNO_H godzinach >= tyle z top 60 coinow mialo RSI 1h < 30
SQUEEZE_OKNO_H = 24             # ...okno wyprzedania (h)
SQUEEZE_RYZYKO_R = 0.5          # ...proponowane ryzyko na kandydata (R) - sygnal nieudowodniony (8 zdarzen w 40 dniach)
SQUEEZE_MAX_SL_PCT = 6.0        # ...kandydat odpada, gdy SL (pod dolkiem 24h) jest dalej niz tyle % (wypompowane / zbyt zmienne)
SQUEEZE_MAX_RANK = 60           # ...tylko top N wg wolumenu
SQUEEZE_LOG = 'squeeze_log.csv' # na Drive: kazde uruchomienie (do pozniejszej oceny sygnalu)
GATE_STATS = True               # V6.13: long/short i likwidacje z Gate.io (inna gielda; tylko informacja + zapis, NIE zmienia ocen)
GATE_TOP_N = 40                 # ...tyle najplynniejszych coinow (+ coiny z setupow, lacznie max GATE_MAX_SYM)
GATE_MAX_SYM = 80
GATE_LOG = 'gate_stats_history.csv'   # na Drive: ostatnia godzina kazdego kontraktu przy kazdym uruchomieniu
DZIENNY_LIMIT_R = 3.0           # V6.11: strata dnia (suma R z dziennika, czas PL) od tylu R -> nic 'do handlu' do konca dnia
DZIENNIK_CSV = 'dziennik.csv'   # na Drive: data_pl (RRRR-MM-DD), symbol, side, wynik_R lub pnl_usdt, zrodlo (skrypt/reczna), setup, uwagi
WYNIK_DNIA_R = None             # 17.10: nowy dzien. 16.10 skonczyl sie na -10R (101 -> 91, wplata +100 -> 190). (wynik dnia w R - wpisz tylko w dniu, ktorego dotyczy)
MAX_RYZYKO_KIERUNEK_PCT = 2.0   # ostrzezenie: laczne ryzyko setupow A/B w jednym kierunku powyzej tylu % kapitalu
CROWD_Z = 2.5                   # CROWD: |funding z| od tylu = tlum mocno po jednej stronie (z historii fundingu do danej chwili)
CROWD_TURN_BARS = 8             # CROWD: zwrot struktury 15m przeciw tlumowi nie starszy niz tyle swiec (2h)
CROWD_EXT_BARS = 32             # CROWD: SL za ekstremum ostatnich tylu swiec 15m (8h)
CROWD_LOG = 'crowd_history.csv' # na Drive: funding/OI/basis kazdego kontraktu przy kazdym uruchomieniu (do przyszlych testow)
CHOP_MAX = 52.7                 # CHOP 1h powyzej (gorna 1/3 w historii) -> -1 pkt (gorzej w obu polowach 30 dni)
WARUNKI_HANDLU = True           # 'do handlu' tylko gdy: rezim 1h trend (SWING4H: trend 4h), BTC 1h nie przeciw,
                                # brak alarmu BTC i trybu ochronnego. Test 14.10: lepiej w obu polowach 30 dni
FILTR_POSCIG = False            # filtr 'poscig' wylaczony: odrzucane przez niego setupy wypadaly lepiej niz przepuszczane
OCENA_TOP_N = 8                 # ile pozycji rankingu na rodzaj wejscia w raporcie
OCENA_MOMENTUM = True           # V6.4: zgodnosc momentum (0-3): cena po stronie transakcji wzgledem VWAP dziennego, RSI 15m
                                # po stronie transakcji (>50 long, <50 short), zmiana 4h w strone transakcji. <= 1 z 3 -> -1 pkt.
                                # Test 14.10: 0-1 z 3 najgorsze w obu probach (30 dni: -0.14/-0.09R, 7 dni: -0.29R; 3 z 3: -0.01/+0.24R)
OCENA_SHORT_SLABY_RS = -2.0     # V6.12: SHORT na coinie slabszym od BTC o wiecej niz tyle % w 24h -> -1 pkt (None = wylaczone).
                                # Badanie 15.10 (setupy po filtrach): takie shorty gorsze od reszty w 4/4 polowach
                                # (archiwum WEEX +0.49/+0.01R vs +0.62/+0.15R; 30 dni -0.11/+0.21R vs +0.41/+0.25R) - juz spadly, czesto odbijaja.
                                # Tylko ocena (nie filtr) - nie zmienia bramki ani testu historycznego
OCENA_FUNDING_Z_MAX = 3.0       # |funding z| powyzej -> najwyzej C (skrajny funding, ryzyko gwaltownego ruchu)
# --- badanie rynku (V6.4): kierunek i przegrzanie, liczone przy kazdym uruchomieniu na swiecach 1h (z archiwum - rosnie)
BADANIE_RYNKU = True            # kierunek.csv + przegrzanie.csv; reguly wlaczaja sie same tylko po tescie w OBU polowach historii
BADANIE_MIN_T = 2.0             # sygnal musi miec t >= tyle w kazdej polowie (t liczone na nienakladajacych sie oknach / dniach)
BADANIE_MIN_BPS = 10.0          # ...i srednio >= tyle bps na transakcje w kazdej polowie
KIER_FILTR = True               # gdy sygnal kierunku przejdzie test: setupy przeciw niemu -> nie do handlu ('kierunek przeciw')
PRZEGRZANIE_FILTR = True        # gdy po przegrzaniu ruch zwykle TRWA dalej (test): setup przeciw przegrzanemu ruchowi -> nie do handlu
PRZEGRZANIE_RSI = 75            # przegrzanie: RSI 1h > tyle (w dol: < 100 - tyle)
PRZEGRZANIE_EXT_ATR = 4.0       # ...albo cena dalej niz tyle ATR 1h od EMA50 1h
PRZEGRZANIE_24H_PCT = 15.0      # ...albo ruch 24h wiekszy niz tyle %
BADANIE_KOSZT = 0.0022          # koszt transakcji w tescie (taker x2 + poslizg), ulamek ceny
# --- cechy ruchu 24h (V6.7) - na razie tylko zapis i test
CECHY_KONTRA_PCT = 20           # kontra/pogon = dolne/gorne tyle % rankingu zmiany 24h w strone transakcji
CECHY_VOL_SKOK = 1.8            # wolumen_skok: vol_ratio_1h (wolumen ostatniej godziny / typowy) od tylu
OCENA_CECHY_RUCHU = False       # True = w ocenie: kontra +1, pogon -1, wolumen_skok +1 (wlacz dopiero po potwierdzeniu w hist_cechy.csv)
OI_MAX_DO_WOLUMENU = 50.0       # OI wiekszy niz tyle x wolumen 24h = blad jednostek WEEX -> OI niewiarygodne
INCREMENTAL_KLINES = True       # V6.8: z aktualnym archiwum swiec na Drive pobieraj tylko nowe swiece
INCR_MARGIN_BARS = 20           # ...plus tyle swiec zakladki (nadpisuje niedomkniete swiece z archiwum)
REPLAY_ZIP = ''                 # sciezka do zipa z wczesniejszego uruchomienia -> analiza bez API, bez zapisu na Drive
# ====================================================================================

import io
import csv
import json
import math
import time
import shutil
import zipfile
import zlib
import threading
from pathlib import Path
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed
import numpy as np
import pandas as pd
import requests
import hmac
import hashlib
import base64
import os
try:
    from scipy.signal import lfilter
except ImportError:
    lfilter = None
try:
    from zoneinfo import ZoneInfo
except ImportError:
    ZoneInfo = None
try:
    from google.colab import files, drive
    IN_COLAB = True
except ImportError:
    IN_COLAB = False

VERSION = 'V6.27'
WEEX = 'https://api-contract.weex.com'
INTERVALS = {'5m': HISTORY_5M_BARS, '15m': 1000, '1h': 1000, '4h': 500}
KLINES_MAX_LIMIT = 1000
KLINES_FALLBACK_LIMIT = 500
FUNDING_LIMIT = 200              # V6.8: 200 stawek (~66 dni przy 8h) - test 30 dni ma funding na calym okresie
FUNDING_LIMIT_FALLBACK = 30
FUNDING_Z_LIVE_N = 30           # funding z na zywo: z tylu ostatnich stawek (jak przed V6.8)
DEPTH_LIMITS = (200, 15)
WORKERS = 6
MAX_REQ_PER_SEC = 10
TIMEOUT = 20
RETRIES = 4
DRIVE_DIR = '/content/drive/MyDrive/weex_collector'
ARCHIVE_NAME = 'backtest_archive_v63.csv'   # V6.4 zmienia tylko filtry (wynik w R bez zmian) -> archiwum V6.3 dalej wazne
OI_HISTORY_NAME = 'oi_history_v63.json'   # OI w USDT; plik V6.1 (oi_history.json) mial inne jednostki
OI_HISTORY_KEEP_DAYS = 90
OUT_ROOT = Path('/content') if IN_COLAB else Path('.')
OUT = OUT_ROOT / 'weex_data'
INTERVAL_MS = {'1m': 60000, '5m': 300000, '15m': 900000, '1h': 3600000, '4h': 14400000}
HOUR_MS = 3600000
DAY_MS = 86400000

# --- parametry price action
FVG_MIN_ATR = 0.1               # min. wielkosc FVG (ATR swojego interwalu)
FVG_WEJSCIE_POZ = 1.0           # miejsce LIMIT w FVG: 1.0 = krawedz blizsza cenie, 0.5 = CE (srodek, jak V6.3), 0.0 = krawedz dalsza.
                                # Test 14.10 (30 dni, 41 kontraktow, warianty na tych samych swiecach): krawedz vs CE
                                # suma +165R vs +122R, lepiej w obu polowach (+20.5R / +22.2R), wypelnienie 77% vs 73%, SL 36% vs 37%
LIMIT_MAX_FILL_PCT = 50.0       # FVG wypelniony bardziej niz to nie nadaje sie na LIMIT
SWEEP_LEVEL_K = 3               # pivot 1h tworzacy poziom plynnosci
REV_SWEEP_WINDOW_15M = 12       # sweep w ostatnich N swiecach 15m (3h)
REV_MSS_MAX_AGE_5M = 36         # CHoCH 5m nie starszy niz N swiec (3h)
CONT_WINDOW_15M = 16            # wybicie w ostatnich N swiecach 15m (4h)
CONT_ACCEPT_BARS = 2            # min. zamkniec 15m za poziomem
CONT_ENTRY_FRONT_ATR = 0.05     # LIMIT tyle ATR przed poziomem (retest nie zawsze dotyka idealnie) - LONG
CONT_ENTRY_FRONT_ATR_SHORT = 0.15   # V6.5: SHORT (test 30 dni: lepiej w obu polowach); None = jak CONT_ENTRY_FRONT_ATR
CONT_MAX_RETESTY = None         # V6.5: max swiec 15m po wybiciu, ktore juz dotknely wejscia (None = bez filtra, tylko kolumna)
CONT_SL_ATR = 0.8               # SL za poziomem, gdy brak struktury 15m
SETUP_MIN_SL_ATR = 0.35
SETUP_MAX_SL_ATR = 3.0
SETUP_SL_BUFFER_ATR = 0.1
SETUP_MIN_TP_ATR = 0.5
SETUP_MAX_RISK_PCT = 5.0
TP_MAX_R = 4.0                  # poziom plynnosci dalej niz tyle R nie jest celem TP1
TP_DEFAULT_R = 2.0              # TP1, gdy brak poziomu plynnosci w zasiegu
TP_FRONT_ATR = 0.05             # TP tyle ATR przed poziomem plynnosci
BTC_CORR_MIN = 0.5              # kontekst BTC liczy sie przy korelacji 1h >= tyle
SESSIONS_UTC = {'azja': (0, 7), 'londyn': (7, 12), 'ny': (12, 20), 'noc': (20, 24)}
# --- jakosc danych / rynku
VOL_ANOMALY_TOP3_SHARE = 0.40
VOL_ANOMALY_MEDIAN_SHARE = 0.12
VOL_ANOMALY_FLAT_SHARE = 0.30
TRADFI_TYPES = {'Stocks', 'Indices', 'Pre-IPO', 'Metals', 'Commodities'}
STOCK_TYPES = {'Stocks', 'Indices', 'Pre-IPO'}   # handel tylko w sesji USA; poza testem wstecznym (luki sesyjne)
PRICE_MISMATCH_PCT = 2.0        # cena tickera vs ostatnia swieca - powyzej: dane niespojne
# --- okna analizy (ile ostatnich swiec bierze analiza) i minimum danych
WIN = {'5m': 400, '15m': 400, '1h': 500, '4h': 200}
MIN_BARS = {'5m': 300, '15m': 200, '1h': 200, '4h': 60}

O, H, L, C, V, QV, TB, T = range(8)   # kolumny macierzy swiec
_thread_local = threading.local()
_paging = {'supported': None}
_clock = {'offset_ms': 0}       # czas serwera - czas lokalny
_qc_lock = threading.Lock()


# ============================== HTTP ==============================
class RateLimiter:

    def __init__(self, per_sec):
        self.interval = 1.0 / per_sec
        self.lock = threading.Lock()
        self.next_time = 0.0

    def wait(self):
        with self.lock:
            now = time.monotonic()
            if self.next_time > now:
                time.sleep(self.next_time - now)
                now = self.next_time
            self.next_time = now + self.interval


limiter = RateLimiter(MAX_REQ_PER_SEC)


def get_session():
    if not hasattr(_thread_local, 'session'):
        s = requests.Session()
        s.headers.update({'User-Agent': 'Mozilla/5.0 market-analysis/6.3'})
        _thread_local.session = s
    return _thread_local.session


_api = {'n': 0}
_api_lock = threading.Lock()


def request_json(base, path, params=None):
    url = base + path
    last_error = None
    for attempt in range(RETRIES):
        try:
            limiter.wait()
            with _api_lock:
                _api['n'] += 1
            response = get_session().get(url, params=params, timeout=TIMEOUT)
            if response.status_code in (403, 451):
                raise PermissionError(f'HTTP {response.status_code}: {url}. Dostep do API moze byc blokowany.')
            if response.status_code == 429 or response.status_code >= 500:
                last_error = f'HTTP {response.status_code}: {response.text[:200]}'
                retry_after = response.headers.get('Retry-After')
                wait = float(retry_after) if retry_after else 1.5 * 2 ** attempt
                time.sleep(min(wait, 20))
                continue
            if 400 <= response.status_code < 500:
                raise ValueError(f'HTTP {response.status_code}: {response.text[:200]}')
            response.raise_for_status()
            data = response.json()
            if isinstance(data, dict) and 'code' in data:
                code = str(data['code'])
                if code not in ('00000', '0', '200'):
                    raise ValueError(f"WEEX code={code}, msg={data.get('msg')}, path={path}")
            return data
        except (PermissionError, ValueError):
            raise
        except Exception as exc:
            last_error = str(exc)
            time.sleep(1 + attempt * 2)
    raise RuntimeError(f'API error: {path}, params={params}: {last_error}')


def weex(path, params=None):
    return request_json(WEEX, path, params)


# ============================== NARZEDZIA ==============================
def save_json(filename, data, pretty=False):
    path = OUT / filename
    path.parent.mkdir(parents=True, exist_ok=True)
    if pretty:
        txt = json.dumps(data, ensure_ascii=False, indent=2, default=json_default)
    else:
        txt = json.dumps(data, ensure_ascii=False, separators=(',', ':'), default=json_default)   # V6.8: koder w C (json.dump do pliku jest ~10x wolniejszy)
    path.write_text(txt, encoding='utf-8')
    return path


_czasy = {}


class etap:
    """V6.8: pomiar czasu etapu -> _czasy (raport + run_meta.json)."""
    def __init__(self, name):
        self.name = name

    def __enter__(self):
        self.t0 = time.time()
        return self

    def __exit__(self, *exc):
        _czasy[self.name] = round(_czasy.get(self.name, 0) + time.time() - self.t0, 1)
        return False


def json_default(o):
    if isinstance(o, (np.integer,)):
        return int(o)
    if isinstance(o, (np.floating,)):
        return None if not np.isfinite(o) else float(o)
    if isinstance(o, np.bool_):
        return bool(o)
    if isinstance(o, np.ndarray):
        return o.tolist()
    return str(o)


def unwrap(data):
    if isinstance(data, dict):
        if 'data' in data:
            return data['data']
        if 'result' in data:
            return data['result']
    return data


def number(value, default=0.0):
    try:
        v = float(value)
        return v if math.isfinite(v) else default
    except (TypeError, ValueError):
        return default


def pick(d, *keys, default=None):
    if not isinstance(d, dict):
        return default
    for k in keys:
        if k in d and d[k] not in (None, ''):
            return d[k]
    return default


def utc_now():
    return datetime.now(timezone.utc).isoformat()


def iso(ms):
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).isoformat()


def snap_iso(ms):
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).strftime('%Y-%m-%dT%H:%M')


def parse_utc(s):
    return int(datetime.strptime(s, '%Y-%m-%dT%H:%M').replace(tzinfo=timezone.utc).timestamp() * 1000)


def czas_pl(ms):
    """Godzina w Polsce (Europe/Warsaw), 'DD.MM HH:MM'; bez zoneinfo - UTC."""
    t = datetime.fromtimestamp(ms / 1000, tz=timezone.utc)
    if ZoneInfo is not None:
        try:
            return t.astimezone(ZoneInfo('Europe/Warsaw')).strftime('%d.%m %H:%M')
        except Exception:
            pass
    return t.strftime('%d.%m %H:%M UTC')


def us_session_open(ms):
    """Sesja gieldowa USA pn-pt 9:30-16:00 czasu Nowego Jorku (bez swiat)."""
    t = datetime.fromtimestamp(ms / 1000, tz=timezone.utc)
    if ZoneInfo is not None:
        t = t.astimezone(ZoneInfo('America/New_York'))
    else:
        t = t.replace(tzinfo=None) - pd.Timedelta(hours=4)
    mins = t.hour * 60 + t.minute
    return t.weekday() < 5 and 570 <= mins < 960


def server_ms():
    try:
        return int(weex('/capi/v3/market/time')['serverTime'])
    except Exception:
        return int(time.time() * 1000)


def rnd(x, n=4):
    if x is None:
        return None
    try:
        x = float(x)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(x):
        return None
    return round(x, n)


def psig(x, sig=6):
    """Zaokraglenie ceny do `sig` cyfr znaczacych."""
    if x is None or not math.isfinite(x) or x == 0:
        return x
    return round(x, sig - 1 - int(math.floor(math.log10(abs(x)))))


# ============================== WSKAZNIKI ==============================
def ema_series(x, n):
    k = 2 / (n + 1)
    if lfilter is not None and len(x) > 1:
        y, _ = lfilter([k], [1, -(1 - k)], x, zi=[x[0] * (1 - k)])
        return y
    out = np.empty_like(x, dtype=float)
    out[0] = x[0]
    for i in range(1, len(x)):
        out[i] = x[i] * k + out[i - 1] * (1 - k)
    return out


def rsi(close, n=14):
    if len(close) < n + 2:
        return None
    d = np.diff(close)
    gain = np.where(d > 0, d, 0.0)
    loss = np.where(d < 0, -d, 0.0)
    ag, al = gain[:n].mean(), loss[:n].mean()
    if lfilter is not None:
        a = 1 / n
        ag = lfilter([a], [1, -(1 - a)], gain[n:], zi=[ag * (1 - a)])[0][-1] if len(gain) > n else ag
        al = lfilter([a], [1, -(1 - a)], loss[n:], zi=[al * (1 - a)])[0][-1] if len(loss) > n else al
    else:
        for i in range(n, len(d)):
            ag = (ag * (n - 1) + gain[i]) / n
            al = (al * (n - 1) + loss[i]) / n
    if al == 0:
        return 100.0
    return 100 - 100 / (1 + ag / al)


def true_range(h, l, c):
    return np.maximum(h[1:] - l[1:], np.maximum(abs(h[1:] - c[:-1]), abs(l[1:] - c[:-1])))


def atr_series(h, l, c, n=14):
    out = np.full(len(c), np.nan)
    if len(c) < n + 1:
        return out
    tr = true_range(h, l, c)
    a = tr[:n].mean()
    out[n] = a
    if lfilter is not None and len(tr) > n:
        k = 1 / n
        out[n + 1:] = lfilter([k], [1, -(1 - k)], tr[n:], zi=[a * (1 - k)])[0]
        return out
    for i in range(n, len(tr)):
        a = (a * (n - 1) + tr[i]) / n
        out[i + 1] = a
    return out


def atr(h, l, c, n=14):
    if len(c) < n + 1:
        return None
    return float(atr_series(h, l, c, n)[-1])


def atr_rank(h, l, c, lookback=120):
    """Percentyl biezacego ATR wzgledem ostatnich `lookback` wartosci (0 = najnizsza zmiennosc)."""
    s = atr_series(h, l, c)
    s = s[np.isfinite(s)][-lookback:]
    if len(s) < 20:
        return None
    return float((s[:-1] < s[-1]).mean() * 100)


def adx(h, l, c, n=14):
    if len(c) < 2 * n + 2:
        return (None, None, None)
    up, dn = h[1:] - h[:-1], l[:-1] - l[1:]
    pdm = np.where((up > dn) & (up > 0), up, 0.0)
    mdm = np.where((dn > up) & (dn > 0), dn, 0.0)
    tr = true_range(h, l, c)
    s_tr, s_p, s_m = tr[:n].sum(), pdm[:n].sum(), mdm[:n].sum()
    dxs, dip, dim = [], 0.0, 0.0
    for i in range(n, len(tr)):
        s_tr = s_tr - s_tr / n + tr[i]
        s_p = s_p - s_p / n + pdm[i]
        s_m = s_m - s_m / n + mdm[i]
        dip = 100 * s_p / s_tr if s_tr else 0.0
        dim = 100 * s_m / s_tr if s_tr else 0.0
        dxs.append(100 * abs(dip - dim) / (dip + dim) if dip + dim else 0.0)
    a = float(np.mean(dxs[:n]))
    for x in dxs[n:]:
        a = (a * (n - 1) + x) / n
    return (a, dip, dim)


def choppiness(h, l, c, n=14):
    """Choppiness Index: > 61.8 = konsolidacja, < 38.2 = trend."""
    if len(c) < n + 2:
        return None
    s = true_range(h, l, c)[-n:].sum()
    rng = h[-n:].max() - l[-n:].min()
    if rng <= 0 or s <= 0:
        return None
    return 100 * math.log10(s / rng) / math.log10(n)


def bb_squeeze(c, n=20, k=2.0, lookback=120):
    """Squeeze = szerokosc Bollingera w najwezszych 20% z `lookback` swiec."""
    if len(c) < lookback + n:
        return None, None
    w = np.lib.stride_tricks.sliding_window_view(c[-(lookback + n - 1):], n)
    m, sd = w.mean(axis=1), w.std(axis=1)
    width = 2 * k * sd / np.where(m > 0, m, 1)
    rank = float((width[:-1] < width[-1]).mean() * 100)
    return rank, rank <= 20


def regime(adx_v, chop_v):
    if adx_v is None or chop_v is None:
        return None
    if adx_v >= 25 and chop_v < 50:
        return 'trend'
    if adx_v < 20 or chop_v > 61.8:
        return 'range'
    return 'mieszany'


def trend_of(c):
    """+1: close > EMA20 > EMA50, -1: odwrotnie, 0: brak."""
    if len(c) < 60:
        return 0
    e20, e50 = ema_series(c, 20)[-1], ema_series(c, 50)[-1]
    return 1 if c[-1] > e20 > e50 else -1 if c[-1] < e20 < e50 else 0


def volume_anomaly(a5m, n=288):
    """Martwe / wash-tradowane kontrakty: wolumen 24h skupiony w kilku swiecach, typowa swieca prawie pusta."""
    a = a5m[-n:]
    v = a[:, V]
    tot = float(v.sum())
    if tot <= 0 or len(v) < 50:
        return True
    top3 = float(np.sort(v)[-3:].sum() / tot)
    med = float(np.median(v) * len(v) / tot)
    flat = float(np.mean((v <= 0) | (a[:, H] <= a[:, L])))
    return bool((top3 >= VOL_ANOMALY_TOP3_SHARE and med <= VOL_ANOMALY_MEDIAN_SHARE) or flat >= VOL_ANOMALY_FLAT_SHARE)


def corr_beta(a, b, n=72):
    """Korelacja i beta zwrotow 1h (ostatnie n swiec) wzgledem BTC, wyrownane po czasie."""
    ta, tb = a[:, T], b[:, T]
    common = np.intersect1d(ta, tb)[-(n + 1):]
    if len(common) < 30:
        return (None, None)
    ca, cb = a[np.isin(ta, common), C], b[np.isin(tb, common), C]
    if len(ca) != len(cb) or (ca <= 0).any() or (cb <= 0).any():
        return (None, None)
    ra, rb = np.diff(np.log(ca)), np.diff(np.log(cb))
    if ra.std() == 0 or rb.std() == 0:
        return (None, None)
    cov = np.cov(ra, rb)
    return (float(cov[0, 1] / math.sqrt(cov[0, 0] * cov[1, 1])), float(cov[0, 1] / cov[1, 1]))


# ============================== PRICE ACTION ==============================
def pivots(h, l, k):
    """Indeksy swing high / swing low (potwierdzone k swiecami po obu stronach)."""
    n = len(h)
    if n < 2 * k + 1:
        return np.array([], dtype=int), np.array([], dtype=int)
    wh = np.lib.stride_tricks.sliding_window_view(h, 2 * k + 1)
    wl = np.lib.stride_tricks.sliding_window_view(l, 2 * k + 1)
    idx = np.arange(k, n - k)
    return idx[h[k:n - k] == wh.max(axis=1)], idx[l[k:n - k] == wl.min(axis=1)]


def structure(h, l, c, k=3):
    """BOS/CHoCH na zamknieciach swiec. Pivot z indeksu p jest znany dopiero na swiecy p+k (bez zagladania)."""
    ph, pl = pivots(h, l, k)
    bias, events = 0, []
    last_sh = last_sl = None
    sh_live = sl_live = False
    ih = il = 0
    for i in range(len(c)):
        while ih < len(ph) and ph[ih] + k <= i:
            last_sh, sh_live = ph[ih], True
            ih += 1
        while il < len(pl) and pl[il] + k <= i:
            last_sl, sl_live = pl[il], True
            il += 1
        if sh_live and c[i] > h[last_sh]:
            events.append((i, 1, 'BOS' if bias == 1 else 'CHoCH', float(h[last_sh])))
            bias, sh_live = 1, False
        if sl_live and c[i] < l[last_sl]:
            events.append((i, -1, 'BOS' if bias == -1 else 'CHoCH', float(l[last_sl])))
            bias, sl_live = -1, False
    label = None
    if len(ph) >= 2 and len(pl) >= 2:
        label = ('HH' if h[ph[-1]] > h[ph[-2]] else 'LH') + '/' + ('HL' if l[pl[-1]] > l[pl[-2]] else 'LL')
    return {'bias': bias, 'events': events, 'ph': ph, 'pl': pl, 'label': label}


def fvgs(h, l, atr_v, side, lookback):
    """FVG w strone `side` (+1 byczy, -1 niedzwiedzi). i = trzecia swieca luki. fill = % wypelnienia do teraz."""
    n = len(h)
    s = max(2, n - lookback)
    if n - s < 1 or not atr_v:
        return []
    i = np.arange(s, n)
    if side == 1:
        bottom, top = h[i - 2], l[i]
        rest = np.append(np.minimum.accumulate(l[::-1])[::-1][1:], np.inf)  # min low PO swiecy i
        worst = rest[i]
    else:
        bottom, top = h[i], l[i - 2]
        rest = np.append(np.maximum.accumulate(h[::-1])[::-1][1:], -np.inf)
        worst = rest[i]
    size = top - bottom
    ok = size >= FVG_MIN_ATR * atr_v
    out = []
    for j in np.where(ok)[0]:
        sz = size[j]
        fill = (top[j] - worst[j]) / sz if side == 1 else (worst[j] - bottom[j]) / sz
        out.append({'i': int(i[j]), 'bottom': float(bottom[j]), 'top': float(top[j]),
                    'ce': float((bottom[j] + top[j]) / 2), 'size': float(sz),
                    'fill_pct': float(min(max(fill, 0.0), 1.0) * 100)})
    return out


def fvg_label():
    return 'CE' if FVG_WEJSCIE_POZ == 0.5 else ('krawedz' if FVG_WEJSCIE_POZ == 1.0 else f'{FVG_WEJSCIE_POZ:g}')


def fvg_px(g, s):
    """Cena wejscia w FVG wg FVG_WEJSCIE_POZ (s=+1 long: blizsza cenie krawedz = top; s=-1 short: bottom)."""
    if FVG_WEJSCIE_POZ == 0.5:
        return g['ce']
    span = g['top'] - g['bottom']
    return g['bottom'] + FVG_WEJSCIE_POZ * span if s == 1 else g['top'] - FVG_WEJSCIE_POZ * span


def untaken_levels(h, l, k):
    """Niezebrane swingi: high, ktorego zaden pozniejszy high nie przekroczyl (i analogicznie low)."""
    ph, pl = pivots(h, l, k)
    if len(h) == 0:
        return [], []
    sufmax = np.append(np.maximum.accumulate(h[::-1])[::-1][1:], -np.inf)
    sufmin = np.append(np.minimum.accumulate(l[::-1])[::-1][1:], np.inf)
    highs = [float(h[p]) for p in ph if h[p] > sufmax[p]]
    lows = [float(l[p]) for p in pl if l[p] < sufmin[p]]
    return highs, lows


def sweep_levels(a1, ws, kind):
    """Poziomy plynnosci znane PRZED oknem sweepu: niezebrane swingi 1h + ekstremum 24h (swiece 1h zamkniete przed ws)."""
    b = a1[a1[:, T] + HOUR_MS <= ws][-120:]
    if len(b) < 30:
        return []
    highs, lows = untaken_levels(b[:, H], b[:, L], SWEEP_LEVEL_K)
    if kind == 'high':
        return sorted(set(highs + [float(b[-24:, H].max())]))
    return sorted(set(lows + [float(b[-24:, L].min())]))


def liquidity_targets(a1, a15, now_ms):
    """Cele TP: niezebrane swingi 1h i 15m, PDH/PDL (jesli dzis jeszcze nie zebrane)."""
    highs, lows = [], []
    for arr, n in ((a1, 240), (a15, 300)):
        hh, ll = untaken_levels(arr[-n:, H], arr[-n:, L], 3)
        highs += hh
        lows += ll
    day0 = (now_ms // DAY_MS) * DAY_MS
    prev = a1[(a1[:, T] >= day0 - DAY_MS) & (a1[:, T] < day0)]
    today = a15[a15[:, T] >= day0]
    if len(prev):
        pdh, pdl = float(prev[:, H].max()), float(prev[:, L].min())
        if not len(today) or today[:, H].max() < pdh:
            highs.append(pdh)
        if not len(today) or today[:, L].min() > pdl:
            lows.append(pdl)
    return sorted(set(highs)), sorted(set(lows))


# ============================== SWIECE ==============================
KLINE_KEYS = (('openTime', 'time', 't', 'ts'), ('open', 'o'), ('high', 'h'), ('low', 'l'), ('close', 'c'),
              ('volume', 'vol', 'v'), ('closeTime', 'T'), ('quoteVolume', 'quoteAssetVolume', 'turnover', 'q'),
              ('trades', 'count', 'n'), ('takerBuyBaseVolume', 'takerBuyBaseAssetVolume', 'takerBuyVolume', 'V'))


def normalize_klines(rows):
    out = []
    for r in rows or []:
        if isinstance(r, dict):
            r = [pick(r, *keys, default=0) for keys in KLINE_KEYS]
        else:
            r = list(r) + [0] * max(0, 10 - len(r))
        out.append(r)
    out.sort(key=lambda r: number(r[0]))
    return out


def rows_array(rows):
    """Wiersze API -> macierz [open, high, low, close, vol, quote_vol, taker_buy_vol, open_time]."""
    if not rows:
        return np.zeros((0, 8))
    try:
        a = np.array([[r[1], r[2], r[3], r[4], r[5], r[7], r[9], r[0]] for r in rows], dtype=float)
        if np.isfinite(a).all():
            return a
    except (TypeError, ValueError):
        pass
    return np.array([[number(r[1]), number(r[2]), number(r[3]), number(r[4]), number(r[5]), number(r[7]),
                      number(r[9]), number(r[0])] for r in rows])


def bar_closed(row, tf, now_ms):
    close_t = number(row[6]) if len(row) > 6 else 0
    if close_t <= 0:
        close_t = number(row[0]) + INTERVAL_MS[tf] - 1
    return close_t < now_ms


def cut(arr, tf, ts, n=None):
    """Swiece zamkniete przed chwila ts (open + interwal <= ts), ostatnie n."""
    i = int(np.searchsorted(arr[:, T], ts - INTERVAL_MS[tf], side='right'))
    return arr[max(0, i - n):i] if n else arr[:i]


# ============================== POBIERANIE ==============================
def fetch_klines(symbol, tf, total):
    path = '/capi/v3/market/klines'
    limit = min(total, KLINES_MAX_LIMIT)
    try:
        rows = normalize_klines(unwrap(weex(path, {'symbol': symbol, 'interval': tf, 'limit': limit})))
    except ValueError:
        rows = normalize_klines(unwrap(weex(path, {'symbol': symbol, 'interval': tf, 'limit': KLINES_FALLBACK_LIMIT})))
    while len(rows) < total and rows and _paging['supported'] is not False:
        oldest = int(number(rows[0][0]))
        try:
            raw = normalize_klines(unwrap(weex(path, {'symbol': symbol, 'interval': tf, 'limit': KLINES_MAX_LIMIT,
                                                      'endTime': oldest - 1})))
        except Exception:
            raw = None
        more = [r for r in raw or [] if int(number(r[0])) < oldest]
        if not more:
            if (raw is None or raw) and _paging['supported'] is None:
                _paging['supported'] = False   # API odrzucilo/zignorowalo endTime (pusta lista = po prostu brak starszych)
            break
        _paging['supported'] = True
        rows = more + rows
    return rows[-total:]


def need_bars(tf, n, known, now_ms):
    """Ile swiec pobrac (0 = wcale). known = (pierwsza, ostatnia) chwila otwarcia w archiwum.
    Wynik po sklejeniu z archiwum jest taki sam jak przy pelnym pobraniu, bo:
    - archiwum siega co najmniej tak daleko, jak pelne pobranie (bez stronicowania WEEX: max 1000 swiec),
    - nowe swiece + INCR_MARGIN_BARS zakladki nadpisuja koniec archiwum,
    - 15m/1h/4h: gdy archiwum ma juz ostatnia zamknieta swiece, nie ma czego pobierac (analiza uzywa tylko zamknietych)."""
    if not known or not INCREMENTAL_KLINES:
        return n
    first, last = known
    ms = INTERVAL_MS[tf]
    full = n if _paging['supported'] else min(n, KLINES_MAX_LIMIT)   # V6.9: bez stronicowania pelne = max 1000
    cur = (now_ms // ms) * ms                       # otwarcie swiecy w trakcie
    if first > cur - (full - 1) * ms:
        return n                                    # archiwum krotsze niz pelne pobranie
    if tf != '5m' and last >= cur - ms:
        return 0                                    # V6.9: ostatnia zamknieta swieca juz w archiwum
    need = int((now_ms - last) // ms) + 1 + INCR_MARGIN_BARS
    return max(50, need) if need <= KLINES_MAX_LIMIT else n


def funding_aktualny(hist, now_ms):
    """V6.9: historia fundingu z pamieci jest aktualna, gdy do nastepnej stawki (ostatnia + typowy odstep) zostalo > 2 min."""
    if not isinstance(hist, list):
        return False
    pts = sorted(int(number(x.get('fundingTime'))) for x in hist if isinstance(x, dict) and number(x.get('fundingTime')))
    if len(pts) < 8:
        return False
    gap = float(np.median(np.diff(pts[-6:])))
    return gap > 0 and now_ms < pts[-1] + gap - 120000


def fetch_coin(symbol, meta, ticker, now_ms, known=None, premium=None, fund=None):
    d = {'symbol': symbol, 'meta': meta, 'ticker': ticker, 'klines': {}, 'errors': {}}
    for tf, n in INTERVALS.items():
        try:
            n_get = need_bars(tf, n, (known or {}).get(tf), now_ms)
            if n_get == 0:
                d.setdefault('klines_przyrost', {})[tf] = 0      # V6.9: swiece z archiwum (main skleja)
                continue
            rows = fetch_klines(symbol, tf, n_get)
            if n_get < n:
                d.setdefault('klines_przyrost', {})[tf] = len(rows)
            t_now = int(time.time() * 1000) + _clock['offset_ms']   # swieca mogla sie zamknac w trakcie pobierania
            d['klines'][tf] = {'data': rows, 'last_closed': bool(rows) and bar_closed(rows[-1], tf, t_now)}
        except Exception as exc:
            d['errors'][f'klines_{tf}'] = str(exc)[:200]
    calls = {'funding_history': ('/capi/v3/market/fundingRate', {'symbol': symbol, 'limit': FUNDING_LIMIT}),
             'premium': ('/capi/v3/market/premiumIndex', {'symbol': symbol}),
             'open_interest': ('/capi/v3/market/openInterest', {'symbol': symbol})}
    if premium:
        d['premium'] = [premium]                  # V6.8: z jednego zapytania dla wszystkich kontraktow
        calls.pop('premium')
    if funding_aktualny(fund, now_ms):
        d['funding_history'] = fund               # V6.9: z pamieci na Drive - od ostatniej stawki nic nowego
        d['funding_z_pamieci'] = True
        calls.pop('funding_history')
    for key, (path, params) in calls.items():
        try:
            d[key] = unwrap(weex(path, params))
        except ValueError as exc:
            if key == 'funding_history' and FUNDING_LIMIT != FUNDING_LIMIT_FALLBACK:
                try:
                    d[key] = unwrap(weex(path, {**params, 'limit': FUNDING_LIMIT_FALLBACK}))
                    continue
                except Exception as exc2:
                    exc = exc2
            d['errors'][key] = str(exc)[:200]
        except Exception as exc:
            d['errors'][key] = str(exc)[:200]
    for lim in DEPTH_LIMITS:
        try:
            d['depth'] = unwrap(weex('/capi/v3/market/depth', {'symbol': symbol, 'limit': lim}))
            break
        except Exception as exc:
            d['errors']['depth'] = str(exc)[:200]
    if 'depth' in d:
        d['errors'].pop('depth', None)
    return d


def premium_all():
    """V6.8: premiumIndex wszystkich kontraktow jednym zapytaniem (jak Binance bez 'symbol'). {} gdy API nie obsluguje."""
    try:
        x = unwrap(weex('/capi/v3/market/premiumIndex'))
    except Exception:
        return {}
    if isinstance(x, list) and len(x) > 20:
        return {p['symbol']: p for p in x if isinstance(p, dict) and p.get('symbol') and p.get('markPrice') is not None}
    return {}


def collect(known=None, fund_cache=None):
    now_ms = server_ms()
    _clock['offset_ms'] = now_ms - int(time.time() * 1000)
    info = unwrap(weex('/capi/v3/market/exchangeInfo'))
    syms = info.get('symbols', []) if isinstance(info, dict) else info
    meta = {s['symbol']: s for s in syms if isinstance(s, dict) and s.get('symbol')}
    tick = unwrap(weex('/capi/v3/market/ticker/24hr'))
    tick = tick if isinstance(tick, list) else [tick]
    tick = {t['symbol']: t for t in tick if isinstance(t, dict) and t.get('symbol') in meta}
    usdt = [s for s in tick if meta[s].get('quoteAsset', 'USDT') == 'USDT'
            and 'PERPETUAL' in str(meta[s].get('contractType', 'PERPETUAL')).upper()]   # tez TRADIFI_PERPETUAL
    usdt.sort(key=lambda s: number(tick[s].get('quoteVolume')), reverse=True)
    is_tf = lambda s_: (meta[s_].get('underlyingType') or 'COIN') in TRADFI_TYPES
    krypto = [s_ for s_ in usdt if not is_tf(s_)][:TOP_N]
    tradfi = [s_ for s_ in usdt if is_tf(s_)][:TRADFI_N] if TRADFI_ON else []
    top = [s for s in krypto + tradfi if number(tick[s].get('quoteVolume')) >= MIN_QUOTE_VOL_USDT]
    chosen = top + [s for s in dict.fromkeys(ALWAYS_INCLUDE) if s in tick and s not in top]
    n_tf = sum(1 for s_ in top if is_tf(s_))
    print(f'Kontrakty USDT: {len(usdt)}, krypto top {TOP_N} + TradFi top {TRADFI_N if TRADFI_ON else 0} wg wolumenu, '
          f'z wolumenem >= {MIN_QUOTE_VOL_USDT / 1e6:.1f}M: {len(top)} (TradFi {n_tf}) -> pobieram {len(chosen)} ...')
    coins = {}
    with ThreadPoolExecutor(max_workers=WORKERS) as ex:
        prem = premium_all()
        if prem:
            print(f'  premiumIndex: jedno zapytanie dla {len(prem)} kontraktow')
        known = known or {}
        if known:
            print(f'  swiece przyrostowo z archiwum Drive: {sum(1 for s in chosen if s in known)}/{len(chosen)} kontraktow')
        fund_cache = fund_cache or {}
        futs = {ex.submit(fetch_coin, s, meta[s], tick[s], now_ms, known.get(s), prem.get(s), fund_cache.get(s)): s
                for s in chosen}
        for k, fut in enumerate(as_completed(futs), 1):
            s = futs[fut]
            try:
                coins[s] = fut.result()
            except PermissionError:
                raise
            except Exception as exc:
                print(f'  {s}: blad {exc}')
            if k % 20 == 0:
                print(f'  pobrano {k}/{len(chosen)}')
    retry = [s for s, d in coins.items() if 'klines_5m' in (d.get('errors') or {})] + [s for s in chosen if s not in coins]
    if retry:                              # V6.10: drugie podejscie dla kontraktow z bledem swiec 5m
        print(f'  ponowne pobieranie ({len(retry)}): {", ".join(retry[:10])}{" ..." if len(retry) > 10 else ""}')
        time.sleep(2)
        for s in retry:
            try:
                d2 = fetch_coin(s, meta[s], tick[s], now_ms, known.get(s), prem.get(s), fund_cache.get(s))
            except Exception as exc:
                print(f'  {s}: blad ponownie ({str(exc)[:80]})')
                continue
            if 'klines_5m' not in d2['errors'] or s not in coins:
                coins[s] = d2
    try:                                   # V6.5: swiezy ticker po pobraniu swiec (pierwszy jest ~6 min starszy)
        t2 = unwrap(weex('/capi/v3/market/ticker/24hr'))
        t2 = {t['symbol']: t for t in (t2 if isinstance(t2, list) else [t2]) if isinstance(t, dict) and t.get('symbol')}
        for sym, d in coins.items():
            if sym in t2:
                d['ticker_live'] = t2[sym]
        if prem:
            p2 = premium_all()                     # swiezy mark/index na koniec (basis)
            for sym, d in coins.items():
                if sym in p2:
                    d['premium'] = [p2[sym]]
    except Exception as exc:
        print(f'  ticker na koniec: blad ({str(exc)[:120]}) - cena z ostatniej swiecy 5m')
    return coins, now_ms


def load_replay(path):
    z = zipfile.ZipFile(path)
    names = z.namelist()
    pref = next((n[:-len('run_meta.json')] for n in names if n.endswith('run_meta.json')), '')
    coins = {}
    for n in names:
        if n.startswith(pref + 'coins/') and n.endswith('.json'):
            d = json.loads(z.read(n))
            coins[d['symbol']] = d
    now_ms = None
    if pref + 'run_meta.json' in names:
        rm = json.loads(z.read(pref + 'run_meta.json'))
        st = rm.get('server_time')
        if st:
            now_ms = int(datetime.fromisoformat(st).timestamp() * 1000)
    if now_ms is None:
        now_ms = max(int(number(d['klines']['5m']['data'][-1][0])) + INTERVAL_MS['5m'] for d in coins.values())
    return coins, now_ms


# ============================== PRZYGOTOWANIE DANYCH ==============================
def prepare(d, now_ms):
    """Surowe dane -> macierze swiec zamknietych, swieca w trakcie, prowizje, typ aktywa."""
    arr, forming = {}, {}
    for tf in ('5m', '15m', '1h', '4h'):
        k = d.get('klines', {}).get(tf)
        if not k or not k.get('data'):
            continue
        a = rows_array(k['data'])
        a = a[np.argsort(a[:, T], kind='stable')]
        keep = np.append(np.diff(a[:, T]) > 0, True)  # bez duplikatow czasu
        a = a[keep]
        if k.get('last_closed') is False and len(a) > 1:
            forming[tf], a = a[-1], a[:-1]
        arr[tf] = a
    meta = d.get('meta') or {}
    taker = number(meta.get('takerFeeRate'), TAKER_FEE_BPS / 1e4) * 1e4 or TAKER_FEE_BPS
    maker = number(meta.get('makerFeeRate'), MAKER_FEE_BPS / 1e4) * 1e4 or MAKER_FEE_BPS
    return {'arr': arr, 'forming': forming, 'fees': (taker, maker),
            'asset_type': meta.get('underlyingType') or 'COIN', 'raw': d}


def live_price(P):
    """Cena na zywo: ticker pobrany po swiecach (V6.5) -> zamkniecie biezacej swiecy 5m -> pierwszy ticker."""
    d = P.get('raw') or {}
    px = number((d.get('ticker_live') or {}).get('lastPrice'))
    if px:
        return px
    form = (P.get('forming') or {}).get('5m')
    if form is not None and form[C] > 0:
        return float(form[C])
    px = number((d.get('ticker') or {}).get('lastPrice'))
    if px:
        return px
    a5 = (P.get('arr') or {}).get('5m')
    return float(a5[-1, C]) if a5 is not None and len(a5) else 0.0


def view(P, ts=None):
    """Okna swiec do analizy: na zywo (ts=None) albo zamkniete przed ts (test wsteczny)."""
    out = {}
    for tf, a in P['arr'].items():
        out[tf] = a[-WIN[tf]:] if ts is None else cut(a, tf, ts, WIN[tf])
    return out


def enough(A):
    return all(tf in A and len(A[tf]) >= MIN_BARS[tf] for tf in MIN_BARS)


# ============================== ANALIZA ==============================
def market_features(A, price, now_ms, btc_A=None, light=False):
    """Cechy rynku liczone wylacznie ze swiec w A (zamknietych) i ceny `price`."""
    a5, a15, a1, a4 = A['5m'], A['15m'], A['1h'], A['4h']
    h1, l1, c1 = a1[:, H], a1[:, L], a1[:, C]
    atr1 = atr(h1, l1, c1)
    e20_1, e50_1 = ema_series(c1, 20)[-1], ema_series(c1, 50)[-1]
    f = {'atr1': atr1, 'atr15': atr(a15[:, H], a15[:, L], a15[:, C]), 'atr5': atr(a5[:, H], a5[:, L], a5[:, C]),
         'e20_1': e20_1, 'e50_1': e50_1}
    f['atr_1h_pct'] = atr1 / price * 100
    f['atr4'] = atr(a4[:, H], a4[:, L], a4[:, C])
    f['ext_ema50_atr_1h'] = (price - e50_1) / atr1
    f['trend_4h'], f['trend_1h'], f['trend_15m'] = trend_of(a4[:, C]), trend_of(c1), trend_of(a15[:, C])
    f['rsi_1h'], f['rsi_15m'] = rsi(c1[-150:]), rsi(a15[-150:, C])
    tail = slice(-150, None)
    adx_v, _, _ = adx(h1[tail], l1[tail], c1[tail])
    chop = choppiness(h1, l1, c1)
    f['adx_1h'], f['chop_1h'], f['regime_1h'] = adx_v, chop, regime(adx_v, chop)
    f['atr_rank_1h'] = atr_rank(h1[-200:], l1[-200:], c1[-200:])
    f['bb_rank_1h'], f['squeeze_1h'] = bb_squeeze(c1)
    o5, v5 = a5[:, O], a5[:, V]
    f['chg_1h_pct'] = (price / o5[-12] - 1) * 100
    f['chg_4h_pct'] = (price / o5[-48] - 1) * 100
    f['chg_24h_pct'] = (price / o5[-288] - 1) * 100 if len(a5) >= 288 else (price / c1[-25] - 1) * 100
    f['chg_7d_pct'] = (price / c1[-169] - 1) * 100 if len(c1) > 169 else None
    base = v5[-288:-12].mean() if len(v5) > 300 else v5[:-12].mean()
    f['vol_ratio_1h'] = v5[-12:].mean() / max(base, 1e-12)
    tb12, v12 = a5[-12:, TB].sum(), v5[-12:].sum()
    f['taker_buy_1h_pct'] = tb12 / v12 * 100 if tb12 > 0 and v12 > 0 else None
    day0 = (now_ms // DAY_MS) * DAY_MS
    dd = a5[a5[:, T] >= day0]
    if len(dd) < 6:
        dd = a5[a5[:, T] >= day0 - DAY_MS]
    tp = (dd[:, H] + dd[:, L] + dd[:, C]) / 3
    vw = (tp * dd[:, V]).sum() / max(dd[:, V].sum(), 1e-12) if len(dd) else None
    f['vwap_d_dist_atr'] = (price - vw) / atr1 if vw else None
    f['vol_anomaly'] = volume_anomaly(a5)
    hour = (now_ms // HOUR_MS) % 24
    f['hour_utc'] = int(hour)
    f['session'] = next(k for k, (a, b) in SESSIONS_UTC.items() if a <= hour < b)
    if btc_A is not None and enough(btc_A):
        b5, b1 = btc_A['5m'], btc_A['1h']
        f['btc_trend_1h'], f['btc_trend_4h'] = trend_of(b1[:, C]), trend_of(btc_A['4h'][:, C])
        bchg = (b5[-1, C] / b5[-288, O] - 1) * 100 if len(b5) >= 288 else (b1[-1, C] / b1[-25, C] - 1) * 100
        f['btc_chg_24h_pct'] = bchg
        f['rs_btc_24h'] = f['chg_24h_pct'] - bchg
        f['corr_btc'], f['beta_btc'] = corr_beta(a1[-100:], b1[-100:])
    st1 = structure(h1[-300:], l1[-300:], c1[-300:], 3)
    st1['offset'] = len(a1) - min(len(a1), 300)
    f['struct_1h_bias'], f['struct_1h'] = st1['bias'], st1['label']
    if st1['events']:
        e = st1['events'][-1]
        f['last_event_1h'] = f"{e[2]}{'+' if e[1] > 0 else '-'} {min(len(a1), 300) - 1 - e[0]}"
    if not light:
        st15 = structure(a15[:, H], a15[:, L], a15[:, C], 3)
        f['struct_15m_bias'], f['struct_15m'] = st15['bias'], st15['label']
        if st15['events']:
            e = st15['events'][-1]
            f['last_event_15m'] = f"{e[2]}{'+' if e[1] > 0 else '-'} {len(a15) - 1 - e[0]}"
    return f, st1


def setup_rev_mss(s, A, f, price):
    """Sweep poziomu 1h na 15m -> CHoCH 5m po ekstremum -> LIMIT w CE FVG z przemieszczenia."""
    a5, a15, a1 = A['5m'], A['15m'], A['1h']
    w15 = a15[-REV_SWEEP_WINDOW_15M:]
    levels = sweep_levels(a1, w15[0, T], 'low' if s == 1 else 'high')
    if not levels:
        return None
    j = lv = None
    for jj in range(len(w15)):
        if s == 1:
            hit = [x for x in levels if w15[jj, L] < x < w15[jj, C]]
        else:
            hit = [x for x in levels if w15[jj, H] > x > w15[jj, C]]
        if hit:
            j, lv = jj, (min(hit) if s == 1 else max(hit))
            break
    if j is None:
        return None
    if (s == 1 and (w15[j:, C] <= lv).any()) or (s == -1 and (w15[j:, C] >= lv).any()):
        return None   # zamkniecie za poziomem po sweepie = sweep sie nie udal
    b5 = int(np.searchsorted(a5[:, T], w15[j, T]))
    seg = a5[b5:]
    if len(seg) < 4:
        return None
    ix = b5 + int(np.argmin(seg[:, L]) if s == 1 else np.argmax(seg[:, H]))
    extreme = float(a5[ix, L] if s == 1 else a5[ix, H])
    lo = max(0, ix - 48)
    ph, pl = pivots(a5[lo:ix, H], a5[lo:ix, L], 2)
    if s == 1:
        ref = float(a5[lo + ph[-1], H]) if len(ph) else float(a5[max(0, ix - 12):ix, H].max())
        hits = np.where(a5[ix + 1:, C] > ref)[0]
    else:
        ref = float(a5[lo + pl[-1], L]) if len(pl) else float(a5[max(0, ix - 12):ix, L].min())
        hits = np.where(a5[ix + 1:, C] < ref)[0]
    if not len(hits):
        return None
    m = ix + 1 + int(hits[0])
    if len(a5) - 1 - m > REV_MSS_MAX_AGE_5M:
        return None
    cands = [g for g in fvgs(a5[:, H], a5[:, L], f['atr5'], s, len(a5) - ix + 2)
             if ix <= g['i'] - 2 and g['i'] <= m + 2 and g['fill_pct'] <= LIMIT_MAX_FILL_PCT]
    tf_fvg = '5m'
    if not cands:
        j15 = len(a15) - len(w15) + j
        t_m = a5[m, T]
        cands = [g for g in fvgs(a15[:, H], a15[:, L], f['atr15'], s, len(a15) - j15 + 2)
                 if g['i'] - 2 >= j15 and a15[g['i'], T] <= t_m + INTERVAL_MS['15m'] and g['fill_pct'] <= LIMIT_MAX_FILL_PCT]
        tf_fvg = '15m'
    if not cands:
        return None
    g = max(cands, key=lambda x: x['size'])
    entry = fvg_px(g, s)
    if (s == 1 and price < g['bottom']) or (s == -1 and price > g['top']):
        return None   # cena przebila FVG na wylot - setup niewazny
    sl = extreme - s * SETUP_SL_BUFFER_ATR * f['atr1']
    body = abs(a5[m, C] - a5[m, O]) / f['atr5'] if f['atr5'] else None
    trig = (f"sweep {'SSL' if s == 1 else 'BSL'} {psig(lv)} (15m) -> CHoCH 5m {'>' if s == 1 else '<'} {psig(ref)} "
            f"({len(a5) - 1 - m} sw. temu) -> LIMIT {fvg_label()} FVG {tf_fvg} {psig(g['bottom'])}-{psig(g['top'])}")
    return {'entry': entry, 'sl': sl, 'trigger': trig, 'sl_basis': 'za ekstremum sweepu',
            'zone': (g['bottom'], g['top']),
            'extra': {'sweep_level': lv, 'mss_age_5m': len(a5) - 1 - m, 'displacement_atr5': body,
                      'fvg_fill_pct': g['fill_pct']}}


def setup_cont_sweep(s, A, f, price):
    """Wybicie poziomu plynnosci zaakceptowane zamknieciami 15m -> LIMIT na retescie poziomu."""
    a15, a1 = A['15m'], A['1h']
    w15 = a15[-CONT_WINDOW_15M:]
    levels = sweep_levels(a1, w15[0, T], 'high' if s == 1 else 'low')
    if not levels:
        return None
    pre_c = a15[-CONT_WINDOW_15M - 1, C]
    j = lv = None
    for jj in range(len(w15)):
        if s == 1:
            hit = [x for x in levels if w15[jj, C] > x and (w15[:jj, C] <= x).all() and pre_c <= x]
        else:
            hit = [x for x in levels if w15[jj, C] < x and (w15[:jj, C] >= x).all() and pre_c >= x]
        if hit:
            j, lv = jj, (max(hit) if s == 1 else min(hit))
            break
    if j is None:
        return None
    after = w15[j:, C]
    if len(after) < CONT_ACCEPT_BARS or (s == 1 and (after <= lv).any()) or (s == -1 and (after >= lv).any()):
        return None
    atr1 = f['atr1']
    front = CONT_ENTRY_FRONT_ATR_SHORT if (s == -1 and CONT_ENTRY_FRONT_ATR_SHORT is not None) else CONT_ENTRY_FRONT_ATR
    entry = lv + s * front * atr1
    if s * (price - lv) <= 0:
        return None   # cena wrocila za poziom - wybicie sie nie utrzymalo
    post = w15[j + 1:]                     # swiece po wybiciu: ile juz dotknelo poziomu wejscia (retest byl)
    retesty = int(((post[:, L] <= entry) if s == 1 else (post[:, H] >= entry)).sum()) if len(post) else 0
    if CONT_MAX_RETESTY is not None and retesty > CONT_MAX_RETESTY:
        return None
    seg = a15[-CONT_WINDOW_15M - 32:]
    ph, pl = pivots(seg[:, H], seg[:, L], 2)
    if s == 1:
        cand = [float(seg[p, L]) for p in pl if entry - 1.5 * atr1 < seg[p, L] < entry - SETUP_MIN_SL_ATR * atr1]
        struct = max(cand) if cand else None
    else:
        cand = [float(seg[p, H]) for p in ph if entry + SETUP_MIN_SL_ATR * atr1 < seg[p, H] < entry + 1.5 * atr1]
        struct = min(cand) if cand else None
    if struct is not None:
        sl, basis = struct - s * SETUP_SL_BUFFER_ATR * atr1, 'za swingiem 15m pod poziomem' if s == 1 else 'za swingiem 15m nad poziomem'
    else:
        sl, basis = lv - s * CONT_SL_ATR * atr1, f'{CONT_SL_ATR} ATR za poziomem'
    trig = (f"wybicie {'BSL' if s == 1 else 'SSL'} {psig(lv)}: {len(after)} zamkn. 15m za poziomem -> "
            f"LIMIT na retescie {psig(entry)}" + (f" (poziom dotkniety juz {retesty}x)" if retesty else ''))
    return {'entry': entry, 'sl': sl, 'trigger': trig, 'sl_basis': basis,
            'zone': (min(lv, entry), max(lv, entry)),
            'extra': {'break_level': lv, 'bars_since_break_15m': len(after), 'retesty_15m': retesty}}


def setup_pullback(s, A, f, st1, price):
    """Trend 1h (struktura + EMA), 4h i BTC nie przeciw -> LIMIT w CE FVG 15m/1h w biezacej nodze."""
    a15, a1 = A['15m'], A['1h']
    if s * (f['e20_1'] - f['e50_1']) <= 0 or st1['bias'] != s or s * (price - f['e50_1']) <= 0:
        return None
    if f['trend_4h'] == -s:
        return None
    if f.get('btc_trend_1h') == -s and (f.get('corr_btc') or 0) >= BTC_CORR_MIN:
        return None
    piv = st1['pl'] if s == 1 else st1['ph']
    if not len(piv):
        return None
    p = st1['offset'] + int(piv[-1])
    swing = float(a1[p, L] if s == 1 else a1[p, H])
    t0 = a1[p, T]
    leg_ext = float(a1[p:, H].max() if s == 1 else a1[p:, L].min())
    atr1 = f['atr1']
    best = None
    for tf, arr, atr_tf in (('15m', a15, f['atr15']), ('1h', a1, atr1)):
        cands = [g for g in fvgs(arr[:, H], arr[:, L], atr_tf, s, 200)
                 if arr[g['i'] - 2, T] >= t0 and g['fill_pct'] <= LIMIT_MAX_FILL_PCT
                 and s * (price - g['ce']) > 0 and abs(price - g['ce']) <= LIMIT_MAX_DIST_ATR * atr1]
        if cands:
            g = max(cands, key=lambda x: s * x['ce'])   # najblizej ceny
            best = (tf, g)
            break
    if best is None:
        return None
    tf, g = best
    entry = fvg_px(g, s)
    sl, basis = swing - s * SETUP_SL_BUFFER_ATR * atr1, 'za swingiem 1h (poczatek nogi)'
    if abs(entry - sl) > SETUP_MAX_SL_ATR * atr1:
        seg = a15[a15[:, T] >= t0]
        ph, pl = pivots(seg[:, H], seg[:, L], 2)
        if s == 1:
            cand = [float(seg[q, L]) for q in pl if seg[q, L] < g['bottom']]
            alt = max(cand) if cand else None
        else:
            cand = [float(seg[q, H]) for q in ph if seg[q, H] > g['top']]
            alt = min(cand) if cand else None
        if alt is None:
            return None
        sl, basis = alt - s * SETUP_SL_BUFFER_ATR * atr1, 'za swingiem 15m w nodze'
    trig = (f"trend 1h {'+' if s == 1 else '-'} (struktura {f.get('struct_1h')}, EMA20/50) -> "
            f"LIMIT {fvg_label()} FVG {tf} {psig(g['bottom'])}-{psig(g['top'])} w nodze od {psig(swing)}")
    return {'entry': entry, 'sl': sl, 'trigger': trig, 'sl_basis': basis, 'leg_target': leg_ext,
            'zone': (g['bottom'], g['top']),
            'extra': {'fvg_tf': tf, 'fvg_fill_pct': g['fill_pct']}}


def funding_z_at(raw, ts):
    """Funding z w chwili ts tylko z rozliczonych stawek do ts: (ostatnia - srednia) / odch. std (min. 8 stawek)."""
    hist = (raw or {}).get('funding_history')
    if not isinstance(hist, list):
        return None
    pts = sorted((int(number(x.get('fundingTime'))), number(x.get('fundingRate'))) for x in hist
                 if isinstance(x, dict) and number(x.get('fundingTime')) and number(x.get('fundingTime')) <= ts)
    if len(pts) < 8:
        return None
    r = np.array([v for _, v in pts[-21:]])
    return float((r[-1] - r.mean()) / r.std()) if r.std() > 0 else None


def setup_crowd(s, A, f, price):
    """Przeciw tlumowi: funding skrajny po stronie -s (|z| >= CROWD_Z) i swiezy zwrot struktury 15m w strone s
    -> LIMIT w CE FVG 5m/15m z tego zwrotu. SL za ekstremum ostatnich 8h, cele: plynnosc + klastry likwidacji tlumu."""
    fz = f.get('funding_z_hist')
    if fz is None or (s == 1 and fz > -CROWD_Z) or (s == -1 and fz < CROWD_Z):
        return None
    a15, a5 = A['15m'], A['5m']
    st = structure(a15[-200:, H], a15[-200:, L], a15[-200:, C], 3)
    if not st['events']:
        return None
    e = st['events'][-1]
    age = min(len(a15), 200) - 1 - e[0]
    if e[1] != s or age > CROWD_TURN_BARS:
        return None
    t_ev = a15[len(a15) - min(len(a15), 200) + e[0], T]
    cands = []
    for tf, arr, atr_tf in (('5m', a5, f['atr5']), ('15m', a15, f['atr15'])):
        for g in fvgs(arr[:, H], arr[:, L], atr_tf, s, 60):
            if arr[g['i'], T] >= t_ev - 2 * INTERVAL_MS['15m'] and g['fill_pct'] <= LIMIT_MAX_FILL_PCT and s * (price - g['ce']) > 0:
                cands.append((tf, g))
        if cands:
            break
    if not cands:
        return None
    tf, g = max(cands, key=lambda x: s * x[1]['ce'])
    seg = a15[-CROWD_EXT_BARS:]
    ext = float(seg[:, L].min() if s == 1 else seg[:, H].max())
    up, dn = f.get('liq', ([], []))
    hh, ll = liquidity_targets(A['1h'], A['15m'], int(a5[-1, T]) + INTERVAL_MS['5m'])
    targets = (sorted(set(hh + [cp for cp, _ in up])), sorted(set(ll + [cp for cp, _ in dn])))
    side_txt = 'shorty' if s == 1 else 'longi'
    trig = (f"tlum: {side_txt} (funding z {fz:+.1f}) -> {e[2]} 15m {'w gore' if s == 1 else 'w dol'} ({age} sw. temu) "
            f"-> LIMIT {fvg_label()} FVG {tf} {psig(g['bottom'])}-{psig(g['top'])}")
    return {'entry': fvg_px(g, s), 'sl': ext - s * SETUP_SL_BUFFER_ATR * f['atr1'], 'trigger': trig,
            'sl_basis': 'za ekstremum 8h', 'zone': (g['bottom'], g['top']), 'targets': targets,
            'extra': {'fvg_tf': tf, 'fvg_fill_pct': g['fill_pct'], 'funding_z_hist': fz}}


def setup_swing4h(s, A, f, price):
    """Swing 4h: trend 4h (EMA20/50 + struktura), BTC 4h nie przeciw -> LIMIT w CE niewypelnionego FVG 4h/1h
    w biezacej nodze 4h. SL za swingiem 4h (poczatek nogi), cele = niezebrane swingi 4h. Wazny 24h, trzymanie do 72h."""
    a4, a1 = A['4h'], A['1h']
    atr4 = f.get('atr4')
    if len(a4) < 60 or not atr4:
        return None
    c4 = a4[:, C]
    e20, e50 = ema_series(c4, 20)[-1], ema_series(c4, 50)[-1]
    if s * (e20 - e50) <= 0 or s * (price - e50) <= 0:
        return None
    w = a4[-200:]
    st4 = structure(w[:, H], w[:, L], w[:, C], 3)
    if st4['bias'] != s:
        return None
    if f.get('btc_trend_4h') == -s and (f.get('corr_btc') or 0) >= BTC_CORR_MIN:
        return None
    piv = st4['pl'] if s == 1 else st4['ph']
    if not len(piv):
        return None
    p = len(a4) - len(w) + int(piv[-1])
    swing = float(a4[p, L] if s == 1 else a4[p, H])
    t0 = a4[p, T]
    leg_ext = float(a4[p:, H].max() if s == 1 else a4[p:, L].min())
    best = None
    for tf, arr, atr_tf in (('4h', a4, atr4), ('1h', a1, f['atr1'])):
        cands = [g for g in fvgs(arr[:, H], arr[:, L], atr_tf, s, 150)
                 if arr[g['i'] - 2, T] >= t0 and g['fill_pct'] <= LIMIT_MAX_FILL_PCT
                 and s * (price - g['ce']) > 0 and abs(price - g['ce']) <= LIMIT_MAX_DIST_ATR * atr4]
        if cands:
            best = (tf, max(cands, key=lambda x: s * x['ce']))
            break
    if best is None:
        return None
    tf, g = best
    hh, ll = untaken_levels(w[:, H], w[:, L], 3)
    trig = (f"trend 4h {'+' if s == 1 else '-'} (struktura 4h, EMA20/50) -> LIMIT {fvg_label()} FVG {tf} "
            f"{psig(g['bottom'])}-{psig(g['top'])} w nodze 4h od {psig(swing)}")
    return {'entry': fvg_px(g, s), 'sl': swing - s * SETUP_SL_BUFFER_ATR * atr4, 'trigger': trig,
            'sl_basis': 'za swingiem 4h (poczatek nogi)', 'leg_target': leg_ext, 'zone': (g['bottom'], g['top']),
            'targets': (sorted(hh), sorted(ll)), 'extra': {'fvg_tf': tf, 'fvg_fill_pct': g['fill_pct']}}


def in_market_zone(s, raw, price, atr1):
    """Cena w strefie wejscia albo najwyzej MARKET_ZONE_ATR przed nia (od strony, z ktorej nadchodzi)."""
    lo, hi = raw['zone']
    if s == 1:
        return lo <= price <= hi + MARKET_ZONE_ATR * atr1
    return lo - MARKET_ZONE_ATR * atr1 <= price <= hi


def finalize(typ, s, raw, f, price, fees, targets, mode='LIMIT'):
    """Ryzyko, cele, R:R netto, koszt w R i filtry. None = setup niepoprawny konstrukcyjnie.
    mode='LIMIT': zlecenie na raw['entry'] (cena musi byc jeszcze przed nim); 'MARKET': wejscie po cenie teraz.
    SWING4H: ograniczenia (min/max SL, odleglosc, TP) w ATR 4h zamiast 1h."""
    atr1 = f['atr4'] if typ == 'SWING4H' and f.get('atr4') else f['atr1']
    market = mode == 'MARKET'
    entry, sl = (price if market else raw['entry']), raw['sl']
    if not market and s * (price - entry) <= 0:
        return None   # cena juz na/za poziomem wejscia - to nie jest LIMIT
    if s * (entry - sl) <= 0:
        return None
    if SL_ZA_KLASTER:
        against0 = (f.get('liq', ([], []))[1] if s == 1 else f.get('liq', ([], []))[0])
        near0 = [cp for cp, _ in against0 if 0 <= s * (sl - cp) <= 0.5 * atr1]
        if near0:
            far = min(near0) if s == 1 else max(near0)
            sl = far - s * SL_KLASTER_BUFOR_ATR * atr1
    if abs(entry - sl) < SETUP_MIN_SL_ATR * atr1:
        sl = entry - s * SETUP_MIN_SL_ATR * atr1
    risk = abs(entry - sl)
    if risk > SETUP_MAX_SL_ATR * atr1 or risk / entry * 100 > SETUP_MAX_RISK_PCT:
        return None
    if abs(price - entry) > LIMIT_MAX_DIST_ATR * atr1:
        return None
    taker, maker = fees
    fee_in = taker if market else maker
    cost_in_sl = entry * (fee_in + taker) / 1e4
    cost_in_tp = entry * (fee_in + maker) / 1e4

    def rr_net(tp):
        return (abs(tp - entry) - cost_in_tp) / (risk + cost_in_sl)

    highs, lows = raw.get('targets') or targets
    lv = sorted(x for x in highs if x > entry) if s == 1 else sorted((x for x in lows if x < entry), reverse=True)
    if raw.get('leg_target') is not None and s * (raw['leg_target'] - entry) > 0:
        lv = sorted(set(lv + [raw['leg_target']]), key=lambda x: s * x)
    tps = [x - s * TP_FRONT_ATR * atr1 for x in lv]
    tps = [x for x in tps if s * (x - entry) >= SETUP_MIN_TP_ATR * atr1]
    tp1, basis = None, None
    for x in tps:
        if abs(x - entry) > TP_MAX_R * risk:
            break
        if rr_net(x) >= SETUP_MIN_RR:
            tp1, basis = x, 'plynnosc'
            break
    if tp1 is None:
        tp1, basis = entry + s * TP_DEFAULT_R * risk, f'{TP_DEFAULT_R}R'
    if rr_net(tp1) < SETUP_MIN_RR:
        return None
    if s * (tp1 - price) <= 0:
        return None   # cena juz za TP1 - zlecenie i tak byloby anulowane przed wejsciem
    if not market and s * (price - entry) >= LIMIT_MAX_PROGRESS * abs(tp1 - entry):
        return None   # cena przeszla juz polowe drogi do TP1 - LIMIT najpewniej zostanie anulowany
    further = [x for x in tps if s * (x - tp1) > 0.5 * risk]
    tp2 = further[0] if further and abs(further[0] - entry) <= 6 * risk else tp1 + s * risk
    cost_r = cost_in_sl / risk
    ext_entry = s * (entry - f['e50_1']) / atr1
    filters = []
    if cost_r > MAX_COST_R:
        filters.append('koszt')
    if FILTR_POSCIG and ext_entry > EXT_CHASE_MAX_ATR:
        filters.append('poscig')
    if (typ, 'LONG' if s == 1 else 'SHORT') in WYLACZONE_TYPY:
        filters.append('typ_wylaczony')
    if s == -1 and f['ext_ema50_atr_1h'] > SHORT_SQUEEZE_EXT_ATR:
        filters.append('squeeze')
    if f['vol_anomaly']:
        filters.append('anomalia_wolumenu')
    if FILTR_AZJA_SHORT and s == -1 and f.get('session') == 'azja':
        filters.append('azja_short')
    if FILTR_SHORT_GODZINY_UTC and s == -1 and f.get('hour_utc') in FILTR_SHORT_GODZINY_UTC:
        filters.append('short_godziny')
    if SWING4H_BEZ_FVG4H and typ == 'SWING4H' and (raw.get('extra') or {}).get('fvg_tf') == '4h':
        filters.append('fvg4h')
    if FILTER_COUNTER_TREND and typ != 'CROWD' and (f.get('trend_4h') == -s or (f.get('btc_trend_1h') == -s and (f.get('corr_btc') or 0) >= BTC_CORR_MIN)):
        filters.append('przeciw_trendowi')
    row = {'setup_type': typ, 'typ_wersja': f"{typ}.v{SETUP_TYPES[typ]}{'.M' if market else ''}",
           'side': 'LONG' if s == 1 else 'SHORT', 'entry_type': mode, 'zone_ref': psig(raw['entry'], 7), 'entry': psig(entry, 7), 'sl': psig(sl, 7), 'tp1': psig(tp1, 7), 'tp2': psig(tp2, 7),
           'rr_tp1_net': rnd(rr_net(tp1), 2), 'risk_pct': rnd(risk / entry * 100, 3), 'sl_atr': rnd(risk / f['atr1'], 2),
           'tp_1R': psig(entry + s * risk, 7) if ZARZADZANIE_1R else None,
           'cost_R': rnd(cost_r, 3), 'dist_entry_atr': rnd(abs(price - entry) / atr1, 2),
           'ext_entry_atr': rnd(ext_entry, 2), 'wait_h': 0 if market else SETUP_WAIT_H[typ],
           'trigger': raw['trigger'] + (' | PO RYNKU: cena juz w strefie' if market else ''),
           'sl_basis': raw['sl_basis'], 'tp1_basis': basis, 'filtr': ';'.join(filters)}
    up, dn = f.get('liq', ([], []))
    clusters = up if s == 1 else dn
    tp_liq = None
    for cp, share in clusters:
        x = cp - s * TP_FRONT_ATR * atr1
        if s * (x - entry) < SETUP_MIN_TP_ATR * atr1:
            continue
        if abs(x - entry) > TP_MAX_R * risk:
            break
        if rr_net(x) >= SETUP_MIN_RR and s * (x - price) > 0:
            tp_liq = (x, share)
            break
    against = dn if s == 1 else up                     # klaster tuz za SL = ryzyko wybicia stopa
    near_sl = [sh for cp, sh in against if 0 <= s * (sl - cp) <= 0.5 * atr1]
    row.update(tp_liq=psig(tp_liq[0], 7) if tp_liq else None, rr_tp_liq=rnd(rr_net(tp_liq[0]), 2) if tp_liq else None,
               liq_tp_share=rnd(tp_liq[1], 1) if tp_liq else None,
               liq_za_sl_share=rnd(max(near_sl), 1) if near_sl else None)
    for k, v in raw.get('extra', {}).items():
        row[k] = rnd(v, 6) if isinstance(v, (int, float, np.floating)) else v
    return row


def leverage_plan(r, P, mult=1.0):
    """Plan pozycji dla setupu: wielkosc z ryzyka, dzwignia preferowana/bezpieczna/do uzycia, likwidacja, depozyt.
    Likwidacja (isolated, przyblizenie): odleglosc od wejscia = 1/dzwignia - MMR - prowizja wyjscia."""
    meta = (P.get('raw') or {}).get('meta') or {}
    entry, sl = float(r['entry']), float(r['sl'])
    s = 1 if r['side'] == 'LONG' else -1
    taker, maker = P['fees']
    market = r['entry_type'] == 'MARKET'
    fee_in = (taker + SLIP_BPS) if market else maker
    cost_frac = (fee_in + taker + SLIP_BPS) / 1e4                  # koszt wejscia + wyjscia przez SL, ulamek wartosci
    sl_frac = abs(entry - sl) / entry
    risk_usdt = KAPITAL_USDT * RYZYKO_NA_TRANSAKCJE_PCT / 100 * mult
    notional = risk_usdt / (sl_frac + cost_frac)
    lev_contract = int(number(meta.get('maxLeverage'), 20)) or 20
    exit_fee = taker / 1e4
    lev_safe = int(math.floor(min(lev_contract, 1 / (LEV_LIQ_SL_MULT * sl_frac + LEV_MMR + exit_fee))))
    lev_min = int(math.ceil(notional / (KAPITAL_USDT * MAX_DEPOZYT_PCT / 100)))
    uwagi = []
    lev = min(int(PREFEROWANA_DZWIGNIA), lev_safe)
    if PREFEROWANA_DZWIGNIA > lev_safe:
        uwagi.append(f'preferowana {PREFEROWANA_DZWIGNIA}x za wysoka: likwidacja blizej niz {LEV_LIQ_SL_MULT}xSL')
    if lev < lev_min:
        if lev_min <= lev_safe:
            lev = lev_min
            uwagi.append(f'podniesiona do {lev_min}x: depozyt przekroczylby {MAX_DEPOZYT_PCT:.0f}% kapitalu')
        else:
            uwagi.append('pozycja za duza na bezpieczna dzwignie - zmniejsz ryzyko % albo pomin')
    lev = max(1, lev)
    liq_frac = 1 / lev - LEV_MMR - exit_fee
    liq = entry * (1 - s * liq_frac) if liq_frac > 0 else None
    qty_prec = int(number(meta.get('quantityPrecision'), 4))
    qty = math.floor(notional / entry * 10 ** qty_prec) / 10 ** qty_prec   # w dol: nie wiecej ryzyka niz zalozone
    if qty < number(meta.get('minOrderSize'), 0):
        uwagi.append('ilosc ponizej minimalnego zlecenia')
    return {'dzwignia_pref': int(PREFEROWANA_DZWIGNIA), 'dzwignia_bezp': lev_safe, 'dzwignia': lev,
            'dzwignia_max_kontrakt': lev_contract, 'likwidacja': psig(liq, 7) if liq else None,
            'likw_vs_sl': rnd(liq_frac / sl_frac, 2) if liq else None,
            'wartosc_usdt': rnd(notional, 2), 'depozyt_usdt': rnd(notional / lev, 2),
            'depozyt_pct': rnd(notional / lev / KAPITAL_USDT * 100, 1), 'ilosc': qty,
            'strata_sl_usdt': rnd(risk_usdt, 2), 'zysk_tp1_usdt': rnd(risk_usdt * (r.get('rr_tp1_net') or 0), 2),
            'dzwignia_uwagi': '; '.join(uwagi)}


_secret_err = {}


def api_secret(name):
    """Sekret z Colab (userdata) albo ze zmiennej srodowiskowej. Nigdy nie wpisuj kluczy w kod."""
    try:
        from google.colab import userdata
        v = userdata.get(name)
        if v:
            return v.strip()
    except Exception as exc:
        kind = type(exc).__name__
        _secret_err[name] = ('brak dostepu notatnika (wlacz przelacznik przy sekrecie)' if 'Access' in kind
                             else 'nie ma takiego sekretu (sprawdz nazwe)' if 'NotFound' in kind else kind)
    return os.environ.get(name)


def weex_private(path, params=None, offset_ms=0):
    """Podpisane zapytanie GET do API kontraktow WEEX (HMAC-SHA256, base64). Tylko odczyt."""
    key, secret, phrase = api_secret('WEEX_API_KEY'), api_secret('WEEX_API_SECRET'), api_secret('WEEX_API_PASSPHRASE')
    if not (key and secret and phrase):
        miss = [n for n, v in (('WEEX_API_KEY', key), ('WEEX_API_SECRET', secret), ('WEEX_API_PASSPHRASE', phrase)) if not v]
        raise PermissionError('sekrety Colab: ' + ', '.join(f"{n} - {_secret_err.get(n, 'pusty')}" for n in miss))
    query = '&'.join(f'{k}={v}' for k, v in (params or {}).items())
    ts = str(int(time.time() * 1000) + offset_ms)
    msg = ts + 'GET' + path + (('?' + query) if query else '')
    sign = base64.b64encode(hmac.new(secret.encode(), msg.encode(), hashlib.sha256).digest()).decode()
    headers = {'ACCESS-KEY': key, 'ACCESS-SIGN': sign, 'ACCESS-PASSPHRASE': phrase, 'ACCESS-TIMESTAMP': ts,
               'Content-Type': 'application/json', 'locale': 'en-US'}
    limiter.wait()
    r = get_session().get(WEEX + path + (('?' + query) if query else ''), headers=headers, timeout=TIMEOUT)
    if r.status_code >= 400:
        raise ValueError(f'HTTP {r.status_code}: {r.text[:200]}')
    data = r.json()
    if isinstance(data, dict) and 'code' in data and str(data['code']) not in ('00000', '0', '200'):
        raise ValueError(f"WEEX code={data['code']}, msg={data.get('msg')}")
    return unwrap(data) if isinstance(data, dict) else data


def fetch_account_positions(drive_ok):
    """Otwarte pozycje + SL/TP z otwartych zlecen warunkowych. Zwraca liste w formacie OTWARTE_POZYCJE."""
    offset = server_ms() - int(time.time() * 1000)
    raw = weex_private('/capi/v3/account/position/allPosition', offset_ms=offset)
    raw = raw if isinstance(raw, list) else (raw or {}).get('list') or []
    try:
        algos = weex_private('/capi/v3/openAlgoOrders', offset_ms=offset)
        algos = algos if isinstance(algos, list) else (algos or {}).get('list') or []
    except Exception as exc:
        print(f'Zlecenia SL/TP: blad ({str(exc)[:120]}) - pozycje bez SL/TP z konta')
        algos = []
    state = {}
    sp = Path(DRIVE_DIR) / POZYCJE_STAN
    if drive_ok and sp.exists():
        try:
            state = json.loads(sp.read_text())
        except Exception:
            state = {}
    out = []
    for p in raw:
        size = abs(number(p.get('size')))
        if size <= 0:
            continue
        sym, side = str(p.get('symbol', '')).upper(), str(p.get('side', '')).upper()
        entry = abs(number(p.get('openValue'))) / size if number(p.get('openValue')) else None
        if not entry or side not in ('LONG', 'SHORT'):
            continue
        sls, tps = [], []
        for a in algos:
            if str(a.get('symbol', '')).upper() != sym or str(a.get('algoStatus', '')).upper() in ('FILLED', 'CANCELED', 'CANCELING'):
                continue
            ps = str(a.get('positionSide', '')).upper()
            if ps and ps != side:
                continue
            ot, trig = str(a.get('orderType', '')).upper(), number(a.get('triggerPrice'))
            closing = a.get('reduceOnly') or a.get('closePosition') or (side == 'LONG') == (str(a.get('side', '')).upper() == 'SELL')
            if closing and trig > 0:
                if ot.startswith('STOP') or ot.startswith('TRAILING'):
                    sls.append(trig)
                elif ot.startswith('TAKE_PROFIT'):
                    tps.append(trig)
            if number(a.get('slTriggerPrice')) > 0:
                sls.append(number(a.get('slTriggerPrice')))
            if number(a.get('tpTriggerPrice')) > 0:
                tps.append(number(a.get('tpTriggerPrice')))
        s_ = 1 if side == 'LONG' else -1
        sl = (max(sls) if s_ == 1 else min(sls)) if sls else None          # najciasniejszy stop
        tp = (min(tps) if s_ == 1 else max(tps)) if tps else None          # najblizszy TP
        pid = str(p.get('id') or f"{sym}_{side}_{p.get('createdTime')}")
        st = state.setdefault(pid, {})
        if sl and not st.get('sl_start'):
            st['sl_start'] = sl
        liq = number(p.get('liquidatePrice'))
        out.append({'symbol': sym, 'side': side, 'entry': entry, 'sl': sl, 'tp': tp, 'sl_start': st.get('sl_start'),
                    'otwarta_utc': iso(int(number(p.get('createdTime'))))[:16] if p.get('createdTime') else None,
                    'dzwignia': number(p.get('leverage')) or None, 'ilosc': size,
                    'likwidacja_api': liq if liq > 0 else None, 'margin': p.get('marginType'),
                    'pnl_api': number(p.get('unrealizePnl')), 'zrodlo': 'konto WEEX'})
    if drive_ok:
        try:
            live_ids = {str(p.get('id')) for p in raw}
            state = {k: v for k, v in state.items() if k in live_ids or not k.isdigit()}
            sp.write_text(json.dumps(state))
        except Exception:
            pass
    return out


def load_positions(drive_ok):
    """Otwarte pozycje z pliku na Drive (gdy jest), inaczej z OTWARTE_POZYCJE."""
    rows = list(OTWARTE_POZYCJE)
    src = 'ustawienia'
    fp = Path(DRIVE_DIR) / POZYCJE_CSV
    if drive_ok and fp.exists():
        try:
            rows = pd.read_csv(fp).to_dict('records')
            src = str(fp)
        except Exception as exc:
            print(f'{POZYCJE_CSV}: blad odczytu ({str(exc)[:100]}) - biore OTWARTE_POZYCJE')
    out = []
    acct = []
    if POZYCJE_Z_KONTA and not REPLAY_ZIP:
        try:
            acct = fetch_account_positions(drive_ok)
            print(f'Pozycje z konta WEEX: {len(acct)}')
        except PermissionError as exc:
            print(f'Pozycje z konta: pominiete ({exc})')
        except Exception as exc:
            print(f'Pozycje z konta: blad ({str(exc)[:200]}) - biore pozycje wpisane recznie')
    manual = {}
    for r in rows:
        r = {k: v for k, v in r.items() if not (isinstance(v, float) and math.isnan(v))}
        if r.get('symbol') and r.get('side'):
            manual[(str(r['symbol']).upper().strip(), str(r['side']).upper().strip())] = r
    merged = []
    for a in acct:                                    # konto wygrywa; reczne uzupelnia braki (np. sl_start, tp)
        m = manual.pop((a['symbol'], a['side']), {})
        row = {**m, **{k: v for k, v in a.items() if v is not None}}
        if m.get('sl_start'):
            row['sl_start'] = m['sl_start']           # SL z otwarcia wpisany recznie ma pierwszenstwo
        merged.append(row)
    rows = merged + list(manual.values())
    if acct:
        src = 'konto WEEX' + (f' + {src}' if manual else '')
    for r in rows:
        r = {k: v for k, v in r.items() if not (isinstance(v, float) and math.isnan(v))}
        if not r.get('symbol') or not r.get('entry') or str(r.get('side', '')).upper() not in ('LONG', 'SHORT'):
            print(f'Pozycja pominieta (brak symbol/side/entry): {r}')
            continue
        r['symbol'] = str(r['symbol']).upper().strip()
        r['side'] = str(r['side']).upper().strip()
        out.append(r)
    if out:
        print(f'Otwarte pozycje: {len(out)} ({src})')
    return out


def manage_position(pos, A, f, price, now_ms, fees, setups):
    """Zalecenie dla otwartej pozycji na podstawie price action po wejsciu. SL wylacznie zaciesniany."""
    s = 1 if pos['side'] == 'LONG' else -1
    entry = float(pos['entry'])
    sl = float(pos['sl']) if pos.get('sl') else None
    sl0 = float(pos['sl_start']) if pos.get('sl_start') else sl
    tp = float(pos['tp']) if pos.get('tp') else None
    atr1 = f['atr1']
    taker, maker = fees
    a15, a5 = A['15m'], A['5m']
    powody, uwagi = [], []
    risk0 = abs(entry - sl0) if sl0 else 0.0
    if risk0 < 0.1 * atr1:
        risk0 = atr1
        uwagi.append('R liczone w ATR 1h (brak sl_start / SL przy wejsciu)')
    R = s * (price - entry) / risk0
    t_open = None
    if pos.get('otwarta_utc'):
        try:
            t_open = parse_utc(str(pos['otwarta_utc'])[:16])
        except ValueError:
            uwagi.append('zly format otwarta_utc (RRRR-MM-DDTHH:MM)')
    since15 = int(np.searchsorted(a15[:, T], t_open, side='left')) if t_open else max(0, len(a15) - 24)
    since5 = int(np.searchsorted(a5[:, T], t_open, side='left')) if t_open else max(0, len(a5) - 72)
    seg = a5[since5:]
    if len(seg):
        best = seg[:, L].min() if s == -1 else seg[:, H].max()
        mfe = s * (best - entry) / risk0
    else:
        mfe = 0.0
    mfe = max(mfe, R)
    # struktura po wejsciu
    st15 = structure(a15[:, H], a15[:, L], a15[:, C], 3)
    ev15 = [e for e in st15['events'] if e[0] >= since15]
    against15 = bool(ev15) and ev15[-1][1] == -s
    a1 = A['1h'][-300:]
    since1 = int(np.searchsorted(a1[:, T], t_open, side='left')) if t_open else len(a1) - 6
    st1 = structure(a1[:, H], a1[:, L], a1[:, C], 3)
    ev1 = [e for e in st1['events'] if e[0] >= since1]
    against1 = bool(ev1) and ev1[-1][1] == -s
    # plynnosc w strone pozycji
    highs, lows = liquidity_targets(A['1h'], A['15m'], now_ms)
    ahead = sorted(x for x in highs if x > price) if s == 1 else sorted((x for x in lows if x < price), reverse=True)
    next_liq = ahead[0] if ahead else None
    # proponowany SL: breakeven i swing 15m po wejsciu (z buforem), min. odstep od ceny
    min_gap = SETUP_MIN_SL_ATR * atr1
    buf = SETUP_SL_BUFFER_ATR * atr1
    be = entry + s * entry * (maker + taker) / 1e4
    ph, pl = st15['ph'], st15['pl']
    piv = [p for p in (pl if s == 1 else ph) if p >= since15 and p + 3 <= len(a15) - 1]
    trail, trail_tf = None, '15m'
    for p in reversed(piv):
        lvl = (a15[p, L] - buf) if s == 1 else (a15[p, H] + buf)
        if s * (price - lvl) >= min_gap:
            trail = float(lvl)
            break
    if trail is None:                                   # brak swingu 15m po wejsciu -> swing 5m
        ph5, pl5 = pivots(a5[:, H], a5[:, L], 3)
        for p in reversed([p for p in (pl5 if s == 1 else ph5) if p >= since5 and p + 3 <= len(a5) - 1]):
            lvl = (a5[p, L] - buf) if s == 1 else (a5[p, H] + buf)
            if s * (price - lvl) >= min_gap:
                trail, trail_tf = float(lvl), '5m'
                break
    cands = []
    if R >= POZ_BE_R and s * (price - be) >= min_gap:
        cands.append((be, f'SL na breakeven: zysk {R:.2f}R >= {POZ_BE_R}R'))
    if R >= POZ_TRAIL_R and trail is not None:
        cands.append((trail, f'SL za swingiem {trail_tf} po wejsciu ({psig(trail, 6)})'))
    if sl is None:
        lvl = trail
        if lvl is None:
            piv_all = [p for p in (pl if s == 1 else ph) if p + 3 <= len(a15) - 1]
            for p in reversed(piv_all):
                x = (a15[p, L] - buf) if s == 1 else (a15[p, H] + buf)
                if s * (price - x) >= min_gap:
                    lvl = float(x)
                    break
        if lvl is not None:
            cands.append((lvl, 'BRAK SL - ustaw stop za struktura 15m'))
    nowy_sl, sl_powod = None, None
    for lvl, why in cands:
        ref = nowy_sl if nowy_sl is not None else sl
        if ref is None or s * (lvl - ref) > 0:          # tylko ciasniej
            nowy_sl, sl_powod = lvl, why
    # decyzja
    akcja, czesc = 'TRZYMAJ', 0
    hours = (now_ms - t_open) / HOUR_MS if t_open else None
    if sl is not None and s * (price - sl) <= 0:
        akcja = 'ZAMKNIJ'; powody.append('cena za Twoim SL - zlecenie SL moglo nie zadzialac')
    elif against1:
        e = ev1[-1]
        akcja = 'ZAMKNIJ'; powody.append(f'{e[2]} 1h przeciw pozycji po wejsciu (poziom {psig(e[3], 6)})')
    elif against15 and R <= 0.3:
        e = ev15[-1]
        akcja = 'ZAMKNIJ'; powody.append(f'{e[2]} 15m przeciw pozycji (poziom {psig(e[3], 6)}), zysk {R:+.2f}R - teza zanegowana')
    elif against15:
        e = ev15[-1]
        akcja, czesc = 'ZAMKNIJ_CZESC', POZ_PARTIAL_PCT
        powody.append(f'{e[2]} 15m przeciw pozycji (poziom {psig(e[3], 6)}) przy zysku {R:+.2f}R - zabezpiecz')
    elif hours is not None and hours >= BT_MAX_HOLD_H and R < 0.5:
        akcja = 'ZAMKNIJ'; powody.append(f'{hours:.0f}h bez postepu ({R:+.2f}R) - test zaklada max {BT_MAX_HOLD_H}h')
    elif tp is not None and s * (tp - price) <= POZ_TP_NEAR_ATR * atr1:
        akcja, czesc = 'ZAMKNIJ_CZESC', POZ_PARTIAL_PCT
        powody.append(f'cena przy TP {psig(tp, 6)}')
    elif next_liq is not None and R >= 1.0 and abs(next_liq - price) <= POZ_TP_NEAR_ATR * atr1:
        akcja, czesc = 'ZAMKNIJ_CZESC', POZ_PARTIAL_PCT
        powody.append(f'cena przy plynnosci {psig(next_liq, 6)} (czesto tam reakcja) przy zysku {R:.2f}R')
    elif R >= POZ_PARTIAL_R:
        akcja, czesc = 'ZAMKNIJ_CZESC', POZ_PARTIAL_PCT
        powody.append(f'zysk {R:.2f}R >= {POZ_PARTIAL_R}R')
    elif mfe >= 1.5 and (mfe - R) >= POZ_GIVEBACK * mfe:
        akcja, czesc = 'ZAMKNIJ_CZESC', POZ_PARTIAL_PCT
        powody.append(f'oddane {mfe - R:.2f}R z {mfe:.2f}R maks. zysku')
    if akcja == 'ZAMKNIJ':
        nowy_sl = None
    if akcja != 'ZAMKNIJ' and nowy_sl is not None:
        if akcja == 'TRZYMAJ':
            akcja = 'PRZESUN_SL'
        powody.append(sl_powod)
    if akcja == 'TRZYMAJ':
        powody.append('struktura po wejsciu nie zlamana' + (f', nastepna plynnosc {psig(next_liq, 6)}' if next_liq else ''))
    opp = [x for x in setups if x['side'] != pos['side'] and not x.get('filtr')]
    if opp:
        uwagi.append('przeciwny setup teraz: ' + ', '.join(sorted({x['setup_type'] for x in opp})))
    liq_px = None
    if pos.get('likwidacja_api'):
        liq_px = float(pos['likwidacja_api'])
        stop = nowy_sl if nowy_sl is not None else sl
        if stop is not None and s * (stop - liq_px) <= 0:
            uwagi.append(f'ALARM: likwidacja {psig(liq_px, 6)} PRZED SL - zmniejsz dzwignie / dodaj depozyt')
    elif pos.get('dzwignia'):
        lev = float(pos['dzwignia'])
        lf = 1 / lev - LEV_MMR - taker / 1e4
        liq_px = entry * (1 - s * lf) if lf > 0 else None
        stop = nowy_sl if nowy_sl is not None else sl
        if liq_px is not None and stop is not None and s * (stop - liq_px) <= 0:
            uwagi.append(f'ALARM: likwidacja {psig(liq_px, 6)} PRZED SL - zmniejsz dzwignie / dodaj depozyt')
    pnl = pos.get('pnl_api')
    if pnl is None and pos.get('ilosc'):
        pnl = s * (price - entry) * float(pos['ilosc'])
    return {'symbol': pos['symbol'], 'side': pos['side'], 'akcja': akcja, 'zamknij_pct': czesc,
            'nowy_sl': psig(nowy_sl, 7) if nowy_sl is not None else None, 'price': price, 'entry': entry, 'sl': sl, 'tp': tp,
            'R_teraz': rnd(R, 2), 'MFE_R': rnd(mfe, 2), 'pnl_usdt': rnd(pnl, 2) if pnl is not None else None,
            'godzin': rnd(hours, 1) if hours is not None else None,
            'zamknij_najpozniej_pl': czas_pl(t_open + BT_MAX_HOLD_H * HOUR_MS) if t_open else None,   # V6.11: limit czasu z testu
            'nastepna_plynnosc': psig(next_liq, 7) if next_liq else None,
            'dist_plynnosc_atr': rnd(abs(next_liq - price) / atr1, 2) if next_liq else None,
            'struktura_15m': st15['label'], 'ostatnie_15m': f"{ev15[-1][2]}{'+' if ev15[-1][1] > 0 else '-'}" if ev15 else None,
            'likwidacja': psig(liq_px, 7) if liq_px else None, 'zrodlo': pos.get('zrodlo', 'recznie'),
            'powody': '; '.join(powody), 'uwagi': '; '.join(uwagi)}


def liq_clusters(a1, price):
    """Szacowane klastry likwidacji: dla kazdej z ostatnich LIQ_HOURS swiec 1h wejscia po cenie typowej z wolumenem,
    likwidacje longow pod cena i shortow nad cena; liczone tylko te, do ktorych cena jeszcze nie doszla.
    Zwraca (nad, pod): listy (cena_klastra, udzial_%) posortowane od najblizszego."""
    b = a1[-LIQ_HOURS:]
    if len(b) < 24 or price <= 0:
        return [], []
    h, l, c, v = b[:, H], b[:, L], b[:, C], b[:, V]
    tp = (h + l + c) / 3
    qv = v * tp
    lo_after = np.append(np.minimum.accumulate(l[::-1])[::-1][1:], np.inf)   # min low PO swiecy j
    hi_after = np.append(np.maximum.accumulate(h[::-1])[::-1][1:], -np.inf)
    lo_after = np.minimum(lo_after, price)
    hi_after = np.maximum(hi_after, price)
    up_p, up_w, dn_p, dn_w = [], [], [], []
    for lev, w in LIQ_LEVERAGES:
        ll = tp * (1 - 1 / lev + LIQ_MMR)
        sl = tp * (1 + 1 / lev - LIQ_MMR)
        m_dn = (ll < lo_after) & (ll > price * (1 - LIQ_RANGE_PCT / 100))
        m_up = (sl > hi_after) & (sl < price * (1 + LIQ_RANGE_PCT / 100))
        dn_p.append(ll[m_dn]); dn_w.append(w * qv[m_dn])
        up_p.append(sl[m_up]); up_w.append(w * qv[m_up])

    def group(ps, ws, side):
        ps, ws = np.concatenate(ps), np.concatenate(ws)
        if not len(ps) or ws.sum() <= 0:
            return []
        width = price * LIQ_BIN_PCT / 100
        k = np.floor((ps - price) / width).astype(int)
        out = []
        for b_id in np.unique(k):
            m = k == b_id
            share = ws[m].sum() / ws.sum() * 100
            if share >= LIQ_MIN_SHARE:
                out.append((float((ps[m] * ws[m]).sum() / ws[m].sum()), float(share)))
        return sorted(out, key=lambda x: side * x[0])
    return group(up_p, up_w, 1), group(dn_p, dn_w, -1)


FEATURE_COLS = ['ext_ema50_atr_1h', 'rsi_1h', 'rsi_15m', 'adx_1h', 'chop_1h', 'regime_1h', 'atr_1h_pct', 'atr_rank_1h',
                'bb_rank_1h', 'squeeze_1h', 'trend_4h', 'trend_1h', 'trend_15m', 'struct_1h_bias', 'chg_1h_pct',
                'chg_4h_pct', 'chg_24h_pct', 'chg_7d_pct', 'rs_btc_24h', 'vol_ratio_1h', 'taker_buy_1h_pct',
                'vwap_d_dist_atr', 'btc_trend_1h', 'btc_trend_4h', 'corr_btc', 'beta_btc', 'session',
                'hour_utc', 'funding_z_hist']   # V6.10: godzina i funding z w chwili setupu (do testu cech)


def analyze(A, price, now_ms, fees, btc_A=None, light=False, crowd=None):
    """Pelna analiza jednego kontraktu: (cechy, lista setupow). Te sama funkcja na zywo i w tescie."""
    f, st1 = market_features(A, price, now_ms, btc_A, light)
    if not f['atr1'] or not f['atr5'] or not f['atr15']:
        return f, []
    targets = liquidity_targets(A['1h'], A['15m'], now_ms)
    f['liq'] = liq_clusters(A['1h'], price)
    f['funding_z_hist'] = (crowd or {}).get('funding_z')
    setups = []
    for s in (1, -1):
        for typ, fn in (('REV_MSS', lambda: setup_rev_mss(s, A, f, price)),
                        ('CONT_SWEEP', lambda: setup_cont_sweep(s, A, f, price)),
                        ('PULLBACK', lambda: setup_pullback(s, A, f, st1, price)),
                        ('SWING4H', lambda: setup_swing4h(s, A, f, price)),
                        ('CROWD', lambda: setup_crowd(s, A, f, price))):
            if typ not in SETUP_TYPES:
                continue
            raw = fn()
            if raw is None:
                continue
            rows = [finalize(typ, s, raw, f, price, fees, targets, 'LIMIT')]
            if MARKET_ENTRIES and in_market_zone(s, raw, price, f['atr1']):
                rows.append(finalize(typ, s, raw, f, price, fees, targets, 'MARKET'))
            for row in rows:
                if row is None:
                    continue
                for k in FEATURE_COLS:
                    v = f.get(k)
                    row[k] = rnd(v, 3) if isinstance(v, (float, np.floating)) else v
                setups.append(row)
    return f, setups


# ============================== DANE TYLKO NA ZYWO ==============================
def book_stats(depth, price):
    out = {'spread_bps': None, 'depth_usdt_0.5pct': None, 'book_imbalance_0.5pct': None, 'slip_buy_bps': None,
           'slip_sell_bps': None}
    if not isinstance(depth, dict):
        return out
    asks = np.array([[number(p), number(q)] for p, q, *_ in depth.get('asks', [])]) if depth.get('asks') else None
    bids = np.array([[number(p), number(q)] for p, q, *_ in depth.get('bids', [])]) if depth.get('bids') else None
    if asks is None or bids is None or not len(asks) or not len(bids):
        return out
    asks, bids = asks[np.argsort(asks[:, 0])], bids[np.argsort(-bids[:, 0])]
    mid = (asks[0, 0] + bids[0, 0]) / 2
    out['spread_bps'] = (asks[0, 0] - bids[0, 0]) / mid * 1e4
    a_in, b_in = asks[asks[:, 0] <= mid * 1.005], bids[bids[:, 0] >= mid * 0.995]
    ad, bd = float((a_in[:, 0] * a_in[:, 1]).sum()), float((b_in[:, 0] * b_in[:, 1]).sum())
    out['depth_usdt_0.5pct'] = ad + bd
    out['book_imbalance_0.5pct'] = (bd - ad) / (bd + ad) if bd + ad > 0 else None

    def slip(levels):
        need, cost, qty = ORDER_SIZE_USDT, 0.0, 0.0
        for p, q in levels:
            take = min(q, (need - cost) / p)
            cost += take * p
            qty += take
            if cost >= need - 1e-9:
                return abs(cost / qty / levels[0, 0] - 1) * 1e4
        return None
    out['slip_buy_bps'], out['slip_sell_bps'] = slip(asks), slip(bids)
    return out


def funding_stats(d):
    out = {'funding_rate': None, 'funding_z': None, 'basis_bps': None}
    hist = d.get('funding_history')
    hist = hist if isinstance(hist, list) else []
    pts = sorted(((number(x.get('fundingTime')), number(x.get('fundingRate'))) for x in hist if isinstance(x, dict)), reverse=True)
    rates = np.array([v for _, v in pts[:FUNDING_Z_LIVE_N]])   # V6.8: najnowsze najpierw, z z ostatnich N (jak przed zmiana limitu)
    prem = d.get('premium')
    prem = prem[0] if isinstance(prem, list) and prem else prem
    if isinstance(prem, dict):
        last = number(pick(prem, 'lastFundingRate', 'fundingRate'), None)
        out['funding_rate'] = last
        mark, idx = number(prem.get('markPrice')), number(prem.get('indexPrice'))
        if mark > 0 and idx > 0:
            out['basis_bps'] = (mark / idx - 1) * 1e4
        if last is not None and len(rates) >= 8 and rates.std() > 0:
            out['funding_z'] = (last - rates.mean()) / rates.std()
    elif len(rates):
        out['funding_rate'] = rates[0]
    return out


def oi_usdt(d, price):
    oi = d.get('open_interest')
    oi = oi[0] if isinstance(oi, list) and oi else oi
    if isinstance(oi, dict):
        v = number(pick(oi, 'openInterest', 'oi', 'amount'))
        return v * price if v > 0 else None
    return None


def oi_changes(hist, now_ms, cur):
    out = {}
    for name, hrs in (('oi_chg_1h_pct', 1), ('oi_chg_4h_pct', 4), ('oi_chg_24h_pct', 24)):
        target = now_ms - hrs * HOUR_MS
        best = None
        for t, v in hist:
            if abs(t - target) <= max(0.35 * hrs * HOUR_MS, 20 * 60000) and (best is None or abs(t - target) < abs(best[0] - target)):
                best = (t, v)
        ok = best and best[1] and cur and 0.2 < cur / best[1] < 5   # skok x5 = inne jednostki / blad danych, nie zmiana OI
        out[name] = (cur / best[1] - 1) * 100 if ok else None
    return out


# ============================== TEST WSTECZNY ==============================
_fund_pts = {}


def funding_points(raw):
    """V6.11: (czasy_ms, stawki) z historii fundingu kontraktu, posortowane; None gdy brak."""
    if not raw:
        return None
    key = id(raw)
    if key in _fund_pts:
        return _fund_pts[key]
    pts = []
    for x in raw.get('funding_history') or []:
        if isinstance(x, dict):
            t, v = number(x.get('fundingTime')), number(x.get('fundingRate'))
            if t and v is not None:
                pts.append((int(t), float(v)))
    out = None
    if pts:
        pts.sort()
        out = (np.array([p[0] for p in pts], dtype=np.int64), np.array([p[1] for p in pts]))
    _fund_pts[key] = out
    return out


def slip_for(P):
    """V6.11: poslizg (bps) dla kontraktu z biezacego booka: baza + polowa spreadu + wplyw zlecenia na book."""
    if not SLIP_DYNAMIC:
        return SLIP_BPS
    if '_slip' in P:
        return P['_slip']
    raw = P.get('raw') or {}
    a = P.get('arr', {}).get('5m')
    price = float(a[-1, C]) if a is not None and len(a) else None
    out = SLIP_BPS
    try:
        if raw.get('depth') and price:
            b = book_stats(raw.get('depth'), price)
            sp = b.get('spread_bps') or 0.0
            dep = b.get('depth_usdt_0.5pct') or 0.0
            out = SLIP_BPS + sp / 2 + (50.0 * ORDER_SIZE_USDT / dep if dep > 0 else SLIP_MAX_BPS)
    except Exception:
        out = SLIP_BPS
    P['_slip'] = float(min(max(out, SLIP_BPS), SLIP_MAX_BPS))
    return P['_slip']


def simulate(a5, ts, row, fees, fund=None):
    """LIMIT: wypelnienie, SL/TP1/timeout na swiecach 5m od ts. MARKET: wejscie po otwarciu pierwszej swiecy od ts.
    Pesymistycznie: SL przed TP w tej samej swiecy."""
    s = 1 if row['side'] == 'LONG' else -1
    entry, sl, tp = row['entry'], row['sl'], row['tp1']
    taker, maker = fees
    i0 = int(np.searchsorted(a5[:, T], ts))
    wait_bars = int(row['wait_h'] * 12)
    hold_bars = SETUP_HOLD_H.get(row.get('setup_type'), BT_MAX_HOLD_H) * 12
    hold_bars_1r = max(hold_bars, (HOLD_PO_1R_H or 0) * 12)
    risk = abs(entry - sl)
    res = {'status': 'oczekuje', 'filled': False, 'r': 0.0, 'mfe_R': None, 'mae_R': None, 'fill_utc': None,
           'exit_utc': None, 'bars_held': None}
    fi = exit_i = st = None
    market = row.get('entry_type') == 'MARKET'
    if market:
        if i0 >= len(a5):
            return res
        fi = i0
        entry = float(a5[i0, O])        # realna cena wejscia (poslizg wzgledem ceny ze zdjecia)
        if s * (entry - sl) > 0:
            risk = abs(entry - sl)
    part_i = None                        # swieca, w ktorej zamknieto czesc przy 1R (SL -> wejscie / trailing)
    r1 = None
    peak = None                          # najlepsza cena po 1R (tylko swiece zamkniete przed biezaca - bez zagladania)
    for i in range(i0, len(a5)):
        hi, lo = a5[i, H], a5[i, L]
        if part_i is None:
            stop = sl
        else:
            stop = entry
            if TRAIL_PO_1R_R:
                stop = max(entry, peak - TRAIL_PO_1R_R * risk) if s == 1 else min(entry, peak + TRAIL_PO_1R_R * risk)
            if TRAIL_STOPNIE:                # schodkowy stop: najlepsza dotad cena (zamkniete swiece) >= prog -> SL na poziom
                best_R = s * (peak - entry) / risk
                lvl = max([b for a_, b in TRAIL_STOPNIE if best_R >= a_] or [0.0])
                cand = entry + s * lvl * risk
                stop = max(stop, cand) if s == 1 else min(stop, cand)
        hit_sl = lo <= stop if s == 1 else hi >= stop
        hit_tp = hi >= tp if s == 1 else lo <= tp
        if fi is None:
            if i - i0 >= wait_bars:
                res.update(status='niewypelniony', exit_utc=snap_iso(a5[i, T]))
                return res
            px = entry * (1 - s * LIMIT_PRZEBICIE_BPS / 1e4)   # V6.11: cena musi PRZEJSC przez wejscie
            if not (lo <= px if s == 1 else hi >= px):
                if hit_tp:
                    res.update(status='anulowany', exit_utc=snap_iso(a5[i, T]))
                    return res
                continue
            fi = i                       # wypelnienie; TP w tej samej swiecy nie liczy sie (nie wiemy, co pierwsze)
            if hit_sl:
                st, exit_i = 'SL', i
                break
            continue
        if hit_sl:
            if part_i is None:
                st = 'SL'
            else:
                st = 'TRAIL' if s * (stop - entry) > 0 else 'BE'
                trail_px = stop
            exit_i = i
            break
        if ZARZADZANIE_1R and part_i is None:
            r1 = entry + s * risk
            if (hi >= r1 if s == 1 else lo <= r1):
                part_i = i                   # 1R osiagniete (SL w tej swiecy sprawdzony wyzej - pesymistycznie)
                peak = hi if s == 1 else lo
        elif part_i is not None:
            peak = max(peak, hi) if s == 1 else min(peak, lo)
        if hit_tp:
            st, exit_i = 'TP1', i
            break
        if i - fi >= (hold_bars_1r if part_i is not None else hold_bars):
            st, exit_i = 'timeout', i
            break
    if fi is None:
        return res                       # dane skonczyly sie przed wypelnieniem
    end = exit_i if exit_i is not None else len(a5) - 1
    seg = a5[fi:end + 1]
    fav = ((seg[:, H] - entry) if s == 1 else (entry - seg[:, L])) / risk
    adv = ((entry - seg[:, L]) if s == 1 else (seg[:, H] - entry)) / risk
    res.update(filled=True, fill_utc=snap_iso(a5[fi, T]), status='otwarty', bars_held=end - fi,
               mfe_R=rnd(min(fav.max(), 99), 2), mae_R=rnd(min(adv.max(), 99), 2))
    if st is None:
        return res                       # pozycja wciaz otwarta - rozstrzygnie sie w kolejnym uruchomieniu
    fee_in = taker if market else maker
    slip = row.get('slip_bps') or SLIP_BPS      # V6.11: poslizg wg pary
    slip_in = slip if market else 0.0
    if st == 'SL':
        pnl, fee = -max(s * (entry - sl), risk), fee_in + taker + slip_in + slip
    elif st == 'TP1':
        pnl, fee = abs(tp - entry), fee_in + maker + slip_in
    elif st == 'BE':
        pnl, fee = 0.0, fee_in + taker + slip_in + slip
    elif st == 'TRAIL':
        pnl, fee = s * (trail_px - entry), fee_in + taker + slip_in + slip
    else:
        pnl, fee = s * (a5[end, C] - entry), fee_in + taker + slip_in + slip
    if part_i is not None:               # czesc zamknieta przy 1R (limit = maker), reszta wg statusu
        k = CZESC_1R
        pnl = k * risk + (1 - k) * pnl
        fee = fee_in + slip_in + k * maker + (1 - k) * (fee - fee_in - slip_in)
        res['czesc_1R'] = True
    fund_frac = 0.0                      # V6.11: funding w czasie trzymania (cala pozycja - przyblizenie przy czesci 1R)
    if FUNDING_W_WYNIKU and fund is not None:
        t_in, t_out = a5[fi, T], a5[end, T] + INTERVAL_MS['5m']
        m = (fund[0] > t_in) & (fund[0] <= t_out)
        fund_frac = s * float(fund[1][m].sum())   # LONG placi dodatnia stawke, SHORT dostaje
    res['funding_R'] = rnd(-fund_frac * entry / risk, 3)
    res.update(status=st, r=rnd((pnl - entry * fee / 1e4 - fund_frac * entry) / risk, 3), exit_utc=snap_iso(a5[end, T]))
    return res


def sim_fixed(a5, ts, s, sl_d, tp_d, fees, fee_in=None, hold_h=None):
    """Wejscie po otwarciu pierwszej swiecy od ts, SL/TP w stalej odleglosci. Wynik w R po kosztach albo None.
    fee_in: prowizja wejscia (domyslnie taker)."""
    i0 = int(np.searchsorted(a5[:, T], ts))
    hold = (hold_h or BT_MAX_HOLD_H) * 12
    hold1 = max(hold, (HOLD_PO_1R_H or 0) * 12)
    if i0 >= len(a5) or sl_d <= 0:
        return None
    e = float(a5[i0, O])
    seg_long = a5[i0:i0 + hold1 + 1]
    seg = seg_long[:hold + 1]
    sl, tp = e - s * sl_d, e + s * tp_d
    hit_sl = seg[:, L] <= sl if s == 1 else seg[:, H] >= sl
    hit_tp = seg[:, H] >= tp if s == 1 else seg[:, L] <= tp
    j_sl = int(np.argmax(hit_sl)) if hit_sl.any() else 10**9
    j_tp = int(np.argmax(hit_tp)) if hit_tp.any() else 10**9
    taker, maker = fees
    fin = taker if fee_in is None else fee_in
    sin = SLIP_BPS if fin >= taker else 0.0       # wejscie po rynku = poslizg
    if ZARZADZANIE_1R:
        r1 = e + s * sl_d
        hit_1 = seg[:, H] >= r1 if s == 1 else seg[:, L] <= r1
        j_1 = int(np.argmax(hit_1)) if hit_1.any() else 10**9
        if j_1 < 10**9 and j_1 < j_sl and j_1 < j_tp:   # 1R przed SL i TP: czesc zamknieta, SL na wejscie
            k = CZESC_1R
            rest = seg_long[j_1 + 1:]
            tp = e + s * tp_d
            # najlepsza cena z swiec PRZED biezaca (od swiecy 1R wlacznie) -> stop dla biezacej swiecy
            if s == 1:
                peak = np.maximum.accumulate(seg_long[j_1:-1, H]) if len(rest) else np.array([])
                stop = np.maximum(e, peak - TRAIL_PO_1R_R * sl_d) if TRAIL_PO_1R_R else np.full(len(rest), e)
                hit_be = rest[:, L] <= stop
                hit_tp_r = rest[:, H] >= tp
            else:
                peak = np.minimum.accumulate(seg_long[j_1:-1, L]) if len(rest) else np.array([])
                stop = np.minimum(e, peak + TRAIL_PO_1R_R * sl_d) if TRAIL_PO_1R_R else np.full(len(rest), e)
                hit_be = rest[:, H] >= stop
                hit_tp_r = rest[:, L] <= tp
            jb = int(np.argmax(hit_be)) if hit_be.any() else 10**9
            jt = int(np.argmax(hit_tp_r)) if hit_tp_r.any() else 10**9
            if j_tp > j_1 and j_tp < 10**9:
                jt = min(jt, j_tp - j_1 - 1)
            if jb == jt == 10**9:
                if len(seg_long) <= hold1:
                    return None
                p2, f2 = s * (seg_long[hold1, C] - e), taker + SLIP_BPS
            elif jb <= jt:
                p2, f2 = s * (stop[jb] - e), taker + SLIP_BPS
            else:
                p2, f2 = tp_d, maker
            pnl = k * sl_d + (1 - k) * p2
            fee = fin + sin + k * maker + (1 - k) * f2
            return (pnl - e * fee / 1e4) / sl_d
    if j_sl == j_tp == 10**9:
        if len(seg) <= hold:
            return None                      # jeszcze otwarta
        pnl, fee = s * (seg[hold, C] - e), fin + taker + sin + SLIP_BPS
    elif j_sl <= j_tp:
        pnl, fee = -sl_d, fin + taker + sin + SLIP_BPS
    else:
        pnl, fee = tp_d, fin + maker + sin
    return (pnl - e * fee / 1e4) / sl_d


def add_controls(rows, PP, ts, atr_at):
    """Losowe wejscie dla porownania: CONTROL_K losowych kontraktow, ten sam kierunek, ta sama CHWILA WEJSCIA
    (dla LIMIT: swieca wypelnienia), SL = ten sam wielokrotnik ATR 1h, TP = ten sam wielokrotnik R, te same prowizje
    co setup. edge_R = wynik setupu - srednia kontroli -> mierzy wybor kontraktu/poziomu, nie kierunek ani koszty."""
    pool = sorted(atr_at)
    for r in rows:
        r['control_R'] = r['edge_R'] = None
        if r['status'] not in FINAL or len(pool) < 3:
            continue
        s = 1 if r['side'] == 'LONG' else -1
        tp_r = abs(r['tp1'] - r['entry']) / abs(r['entry'] - r['sl'])
        rng = np.random.default_rng(zlib.crc32(f"{r['snapshot_utc']}|{r['symbol']}|{r['typ_wersja']}|{r['side']}".encode()))
        picks = rng.choice(pool, size=min(CONTROL_K, len(pool)), replace=False)
        res = []
        t_in = ts
        if r.get('entry_type') == 'LIMIT' and r.get('fill_utc'):
            t_in = parse_utc(r['fill_utc'])
        for sym in picks:
            sl_d = r['sl_atr'] * atr_at[sym]
            fees = PP[sym]['fees']
            x = sim_fixed(PP[sym]['arr']['5m'], t_in, s, sl_d, tp_r * sl_d, fees,
                          fee_in=fees[1] if r.get('entry_type') == 'LIMIT' else fees[0],
                          hold_h=SETUP_HOLD_H.get(r.get('setup_type'), BT_MAX_HOLD_H))
            if x is not None:
                res.append(x)
        if res:
            r['control_R'] = rnd(float(np.mean(res)), 3)
            r['edge_R'] = rnd(r['r'] - r['control_R'], 3)


def run_backtest(PP, now_ms, lookback_h=None, quiet=False, oi_arr=None, gidx=None):
    lookback_h = lookback_h or BT_LOOKBACK_H
    oi_arr, gidx = oi_arr or {}, gidx or {}
    btc = PP.get('BTCUSDT')
    step = BT_STEP_H * HOUR_MS
    last_snap = (now_ms // step) * step
    snaps = [last_snap - k * step for k in range(int(lookback_h / BT_STEP_H), -1, -1)]
    out, seen = [], set()
    t0 = time.time()
    for n, ts in enumerate(snaps):
        btc_A = view(btc, ts) if btc else None
        snap_rows = []
        chg, atr_at, qv24 = {}, {}, {}
        for sym, P in PP.items():
            if '5m' not in P['arr'] or (P['asset_type'] in STOCK_TYPES and not (TRADFI_ON and us_session_open(ts))):
                continue                                   # V6.14: akcje/indeksy tylko w sesji USA
            A = view(P, ts)
            if not enough(A):
                continue
            price = float(A['5m'][-1, C])
            f, setups = analyze(A, price, ts, P['fees'], btc_A if sym != 'BTCUSDT' else None, light=True,
                                crowd={'funding_z': funding_z_at(P.get('raw'), ts)})
            chg[sym] = f.get('chg_24h_pct')
            if len(A['1h']) >= 24:
                qv24[sym] = float(A['1h'][-24:, QV].sum())   # V6.10: wolumen 24h w chwili zdjecia -> vol_rank w tescie
            if f.get('atr1'):
                atr_at[sym] = f['atr1']
            for r in setups:
                key = (sym, r['typ_wersja'], r['side'], r['zone_ref'])   # ta sama strefa wejscia = ta sama transakcja
                if key in seen:
                    continue
                seen.add(key)
                r.update(snapshot_utc=snap_iso(ts), symbol=sym, asset_type=P['asset_type'],
                         slip_bps=rnd(slip_for(P), 2), metoda_sym='v611')
                if oi_arr:
                    r.update(oi_changes_at(oi_arr.get(sym), ts))      # V6.13: tylko zapis
                if sym in gidx:
                    r.update(gate_features(gidx[sym], ts))
                    gate_side_flags(r)
                fund = funding_points(P.get('raw'))
                r.update(simulate(P['arr']['5m'], ts, r, P['fees'], fund))
                if r.get('tp_liq'):
                    alt = simulate(P['arr']['5m'], ts, {**r, 'tp1': r['tp_liq']}, P['fees'], fund)
                    r['status_liq'], r['r_liq'] = alt['status'], alt['r']
                snap_rows.append(r)
        ranks = rank_pct(chg)
        vr = {x: i + 1 for i, x in enumerate(sorted(qv24, key=lambda k: -qv24[k]))}
        for r in snap_rows:
            r['rank_24h'] = ranks.get(r['symbol'])
            r['vol_rank'] = vr.get(r['symbol'])
            cechy_ruchu(r)
        add_controls(snap_rows, PP, ts, atr_at)
        out += snap_rows
        if not quiet and (n + 1) % 12 == 0:
            print(f'  test: {n + 1}/{len(snaps)} zdjec, {len(out)} setupow, {time.time() - t0:.0f}s')
    return out


def rank_pct(d):
    vals = {k: v for k, v in d.items() if v is not None}
    if len(vals) < 10:
        return {}
    s = pd.Series(vals).rank(pct=True) * 100
    return {k: round(float(v), 1) for k, v in s.items()}


# ============================== DLUGI TEST HISTORYCZNY (inna gielda) ==============================
HIST_TF = {'5m': '5', '15m': '15', '1h': '60', '4h': '240'}       # interwaly Bybit
HIST_WARMUP_D = {'5m': 2, '15m': 3, '1h': 22, '4h': 36}            # dni swiec przed pierwszym zdjeciem (okna analizy)
HIST_NAME = 'hist_backtest_v63.csv'


def hist_fetch(source, symbol, tf, start_ms, end_ms):
    """Swiece zamkniete z [start_ms, end_ms) z Bybit (perp) albo Binance (spot), w formacie wierszy WEEX."""
    rows = []
    if source == 'weex':
        end = end_ms
        while True:
            d = unwrap(weex('/capi/v3/market/historyKlines', {'symbol': symbol, 'interval': tf, 'endTime': end,
                                                              'startTime': max(start_ms, end - _weex_hist['limit'] * INTERVAL_MS[tf]),
                                                              'limit': _weex_hist['limit']}))
            batch = [r for r in normalize_klines(d if isinstance(d, list) else []) if int(number(r[0])) <= end]
            if not batch:
                break
            rows += batch
            oldest = int(number(batch[0][0]))
            if oldest <= start_ms or len(batch) < 2:
                break
            end = oldest - 1
    elif source == 'bybit':
        end = end_ms
        while True:
            d = request_json('https://api.bybit.com', '/v5/market/kline',
                             {'category': 'linear', 'symbol': symbol, 'interval': HIST_TF[tf], 'end': end, 'limit': 1000})
            if not isinstance(d, dict) or str(d.get('retCode')) != '0':
                raise ValueError(f"bybit {symbol}: {d.get('retMsg') if isinstance(d, dict) else d}")
            lst = (d.get('result') or {}).get('list') or []
            if not lst:
                break
            batch = [[int(x[0]), x[1], x[2], x[3], x[4], x[5], int(x[0]) + INTERVAL_MS[tf] - 1, x[6], 0, 0] for x in lst]
            rows += batch
            oldest = min(b[0] for b in batch)
            if oldest <= start_ms or len(lst) < 2:
                break
            end = oldest - 1
    else:
        start = start_ms
        while True:
            d = request_json('https://data-api.binance.vision', '/api/v3/klines',
                             {'symbol': symbol, 'interval': tf, 'startTime': start, 'endTime': end_ms, 'limit': 1000})
            if not isinstance(d, list) or not d:
                break
            rows += [list(x[:10]) for x in d]
            newest = int(d[-1][0])
            if newest + INTERVAL_MS[tf] >= end_ms or newest + 1 <= start:
                break
            start = newest + 1
    rows = normalize_klines(rows)
    out, last = [], None
    for r in rows:
        t = int(number(r[0]))
        if t != last and start_ms <= t and t + INTERVAL_MS[tf] <= end_ms:
            out.append(r)
            last = t
    return out


def hist_symbol_data(source, sym, now_ms):
    """Wszystkie interwaly jednego kontraktu; dla 1000XXX probuje tez XXX (inna skala ceny nie zmienia setupow w ATR)."""
    for name in [sym] + ([sym[4:]] if sym.startswith('1000') else []):
        try:
            kl = {}
            for tf in HIST_TF:
                start = now_ms - (HIST_DAYS + HIST_WARMUP_D[tf]) * DAY_MS
                kl[tf] = hist_fetch(source, name, tf, start, now_ms)
                if len(kl[tf]) < MIN_BARS[tf]:
                    raise ValueError(f'za malo swiec {tf}')
            return kl
        except PermissionError:
            raise
        except Exception:
            continue
    return None


_weex_hist = {'limit': 1000}


def probe_weex_history(now_ms):
    """Czy WEEX historyKlines oddaje starsze swiece z zakresu dat? Okno 5 dni sprzed 25-30 dni (1h)."""
    for lim in (1000, 500):
        _weex_hist['limit'] = lim
        try:
            rows = hist_fetch('weex', 'BTCUSDT', '1h', now_ms - 30 * DAY_MS, now_ms - 25 * DAY_MS)
            if len(rows) >= 100:
                return True
        except Exception as exc:
            last = str(exc)[:120]
            continue
    print(f"  WEEX historyKlines: brak starszych swiec{' (' + last + ')' if 'last' in locals() else ''}")
    return False


def archive_covers(PP, now_ms):
    """Archiwum swiec WEEX obejmuje HIST_DAYS (+ rozruch) dla BTC i wiekszosci kontraktow?"""
    need = now_ms - (HIST_DAYS + HIST_WARMUP_D['5m']) * DAY_MS
    ok = [s for s, P in PP.items() if P['asset_type'] == 'COIN' and '5m' in P['arr'] and len(P['arr']['5m'])
          and P['arr']['5m'][0, T] <= need]
    return 'BTCUSDT' in ok and len(ok) >= min(HIST_COINS, 20)


def hist_pick_source(now_ms, PP=None):
    order = ['weex_archive', 'weex', 'bybit', 'binance'] if HIST_SOURCE == 'auto' else [HIST_SOURCE]
    for src in order:
        try:
            if src == 'weex_archive':
                if PP is not None and archive_covers(PP, now_ms):
                    return src
                continue
            if src == 'weex':
                if probe_weex_history(now_ms):
                    return src
                continue
            if len(hist_fetch(src, 'BTCUSDT', '1h', now_ms - DAY_MS, now_ms)) >= 20:
                return src
        except Exception as exc:
            print(f'  zrodlo {src} niedostepne: {str(exc)[:120]}')
    return None


def hist_signature():
    keys = ('SETUP_TYPES', 'SETUP_WAIT_H', 'MARKET_ENTRIES', 'MARKET_ZONE_ATR', 'MAX_COST_R', 'EXT_CHASE_MAX_ATR',
            'FILTER_COUNTER_TREND', 'LIMIT_MAX_PROGRESS', 'SLIP_BPS',
            'SHORT_SQUEEZE_EXT_ATR', 'SETUP_MIN_RR', 'BT_STEP_H', 'BT_MAX_HOLD_H', 'CONTROL_K', 'HIST_DAYS', 'HIST_COINS')
    keys = keys + ('CONT_ENTRY_FRONT_ATR_SHORT', 'CONT_MAX_RETESTY', 'LIMIT_MAX_FILL_PCT')
    keys = keys + ('FVG_WEJSCIE_POZ', 'CONT_ENTRY_FRONT_ATR', 'SETUP_SL_BUFFER_ATR', 'SETUP_MIN_SL_ATR', 'CONT_SL_ATR',
                   'TP_DEFAULT_R', 'TP_MAX_R', 'SL_ZA_KLASTER', 'SL_KLASTER_BUFOR_ATR')
    return json.dumps({**{k: globals()[k] for k in keys}, 'kontrola': 'v2-chwila-wejscia', 'liq': 'v1', 'fix5': 'v1', 'konfluencje': 'v1', 'zarzadzanie_1R': ZARZADZANIE_1R, 'czesc_1R': CZESC_1R, 'trail_1R': TRAIL_PO_1R_R, 'hold_1R': HOLD_PO_1R_H, 'azja_short': FILTR_AZJA_SHORT, 'short_godziny': list(FILTR_SHORT_GODZINY_UTC), 'fvg4h': SWING4H_BEZ_FVG4H, 'wylaczone': sorted(map(list, WYLACZONE_TYPY)), 'funding': ['v2', FUNDING_LIMIT], 'v611': [LIMIT_PRZEBICIE_BPS, FUNDING_W_WYNIKU, SLIP_DYNAMIC, SLIP_MAX_BPS, ORDER_SIZE_USDT]}, sort_keys=True, default=str)   # 'konfluencje': cache testu z 14.10 nadal wazny


def hist_fetch_all(PP, now_ms):
    """Swiece do testu historycznego (raz na przebieg, wspolne dla wariantow). -> (PPh, zrodlo)"""
    t0 = time.time()
    src = hist_pick_source(now_ms, PP)
    if src is None:
        return {}, None
    syms = [s for s, P in sorted(PP.items(), key=lambda kv: -number((kv[1]['raw'].get('ticker') or {}).get('quoteVolume')))
            if P['asset_type'] == 'COIN'][:HIST_COINS]
    if 'BTCUSDT' in PP and 'BTCUSDT' not in syms:
        syms.append('BTCUSDT')
    print(f'Test historyczny: {src}, {len(syms)} kontraktow, {HIST_DAYS} dni ...')
    PPh = {}
    if src == 'weex_archive':
        PPh = {s: {**PP[s], 'forming': {}} for s in syms if s in PP}
    with ThreadPoolExecutor(max_workers=WORKERS if src != 'weex_archive' else 1) as ex:
        futs = {} if src == 'weex_archive' else {ex.submit(hist_symbol_data, src, s, now_ms): s for s in syms}
        for fut in as_completed(futs):
            s = futs[fut]
            try:
                kl = fut.result()
            except Exception:
                kl = None
            if kl:
                PPh[s] = {'arr': {tf: rows_array(rows) for tf, rows in kl.items()}, 'forming': {},
                          'fees': PP[s]['fees'], 'asset_type': 'COIN',
                          'raw': {'funding_history': (PP[s].get('raw') or {}).get('funding_history'),   # V6.8: CROWD w tescie
                                  'depth': (PP[s].get('raw') or {}).get('depth')}}                       # V6.11: poslizg wg pary
    print(f'  pobrano {len(PPh)}/{len(syms)} kontraktow w {time.time() - t0:.0f}s')
    return PPh, src


def _with_settings(over, fn):
    """Uruchamia fn() z tymczasowo zmienionymi ustawieniami (globalne), zawsze przywraca."""
    g = globals()
    old = {k: g[k] for k in over}
    try:
        g.update(over)
        return fn()
    finally:
        g.update(old)


def variant_table(name, over, df, base=None):
    """Podsumowanie wariantu (setupy po filtrach): calosc i obie polowy okresu + roznica do bazy."""
    if not len(df):
        return []
    ok = df[(df.filtr.fillna('') == '') & df.status.isin(FINAL)].copy()
    if not len(ok):
        return []
    snaps = sorted(df.snapshot_utc.unique())
    mid = snaps[len(snaps) // 2]
    days = max(1.0, len(snaps) * BT_STEP_H / 24)
    out = []
    groups = [('WSZYSTKIE', ok)] + [(f'{tv} {sd}', g) for (tv, sd), g in ok.groupby(['typ_wersja', 'side'])]
    for label, g in groups:
        st = stats(g)
        p1, p2 = g[g.snapshot_utc < mid], g[g.snapshot_utc >= mid]
        row = {'wariant': name, 'zmiana': json.dumps(over) if over else 'obecne ustawienia', 'grupa': label,
               'transakcje': st.get('transakcje'), 'wypelnienie_pct': stats(df[df.typ_wersja.isin(g.typ_wersja.unique())]).get('wypelnienie_pct'),
               'SL_pct': rnd((g.status == 'SL').mean() * 100, 1), 'sredni_R': st.get('sredni_R'), 'suma_R': st.get('suma_R'),
               'R_na_dzien': rnd(g.r.sum() / days, 2), 'sredni_edge_R': st.get('sredni_edge_R'), 't_edge': st.get('t_edge'),
               'p1_sredni_R': rnd(p1.r.mean(), 3) if len(p1) else None, 'p1_suma_R': rnd(p1.r.sum(), 1),
               'p2_sredni_R': rnd(p2.r.mean(), 3) if len(p2) else None, 'p2_suma_R': rnd(p2.r.sum(), 1)}
        if base is not None:
            b = base[base.grupa == label]
            if len(b):
                b = b.iloc[0]
                row['roznica_suma_R'] = rnd((row['suma_R'] or 0) - (b['suma_R'] or 0), 1)
                row['roznica_p1_suma_R'] = rnd((row['p1_suma_R'] or 0) - (b['p1_suma_R'] or 0), 1)
                row['roznica_p2_suma_R'] = rnd((row['p2_suma_R'] or 0) - (b['p2_suma_R'] or 0), 1)
                row['lepszy_w_obu_polowach'] = bool(row['roznica_p1_suma_R'] > 0 and row['roznica_p2_suma_R'] > 0)
        out.append(row)
    return out


def run_hist_variants(PPh, now_ms, base_df, variants=None, quiet=True):
    """Te same swiece, inne ustawienia: tabela porownawcza wariantow vs obecne ustawienia."""
    variants = HIST_WARIANTY if variants is None else variants
    base = pd.DataFrame(variant_table('BAZA', {}, base_df))
    rows = list(base.to_dict('records'))
    for name, over in (variants or {}).items():
        t0 = time.time()
        try:
            res = _with_settings(over, lambda: run_backtest(PPh, now_ms, lookback_h=HIST_DAYS * 24, quiet=True))
        except Exception as exc:
            print(f'  wariant {name}: blad ({str(exc)[:120]})')
            continue
        rows += variant_table(name, over, pd.DataFrame(res), base)
        if not quiet:
            print(f'  wariant {name}: {len(res)} setupow w {time.time() - t0:.0f}s')
    return pd.DataFrame(rows)


def run_hist(PP, now_ms, drive_ok):
    """Test definicji setupow na HIST_DAYS dniach swiec (prowizje WEEX) + warianty ustawien (HIST_WARIANTY).
    Wynik w cache na HIST_REFRESH_H. Zwraca (df, meta); tabela wariantow w meta['_warianty']."""
    cache = Path(DRIVE_DIR) if drive_ok else OUT_ROOT / 'weex_cache'
    cache.mkdir(parents=True, exist_ok=True)
    meta_p, data_p = cache / 'hist_meta_v63.json', cache / HIST_NAME
    var_p = cache / 'hist_warianty_v64.csv'
    sig = hist_signature()
    var_sig = json.dumps(HIST_WARIANTY, sort_keys=True, default=str)
    df = m = None
    if meta_p.exists() and data_p.exists():
        try:
            mm = json.loads(meta_p.read_text())
            if mm.get('sig') == sig and now_ms - mm.get('computed_ms', 0) < HIST_REFRESH_H * HOUR_MS:
                df, m = pd.read_csv(data_p), mm
                print(f"Test historyczny: z cache ({m.get('source')}, {iso(m['computed_ms'])[:16]} UTC)")
        except Exception:
            df = m = None
    var_df = None
    if m is not None and m.get('var_sig') == var_sig and var_p.exists():
        try:
            var_df = pd.read_csv(var_p)
        except Exception:
            var_df = None
    if df is not None and (var_df is not None or not HIST_WARIANTY):
        m['_warianty'] = var_df
        return df, m
    t0 = time.time()
    PPh, src = hist_fetch_all(PP, now_ms)
    if src is None:
        print('Test historyczny: brak zrodla swiec (archiwum WEEX, WEEX, Bybit, Binance) - pomijam')
        if df is not None:
            return df, m
        return pd.DataFrame(), {'source': None}
    if df is None:
        print('  licze test historyczny ...')
        df = pd.DataFrame(run_backtest(PPh, now_ms, lookback_h=HIST_DAYS * 24, quiet=True))
        if len(df):
            df['source'] = src
        m = {'sig': sig, 'computed_ms': now_ms, 'source': src, 'coins': len(PPh), 'seconds': round(time.time() - t0)}
        df.to_csv(data_p, index=False)
        print(f'  test historyczny gotowy: {len(df)} setupow w {time.time() - t0:.0f}s')
    if HIST_WARIANTY and len(df):
        print(f'  warianty ustawien ({len(HIST_WARIANTY)}) na tych samych swiecach ...')
        var_df = run_hist_variants(PPh, now_ms, df, quiet=False)
        var_df.to_csv(var_p, index=False)
        m['var_sig'] = var_sig
    meta_p.write_text(json.dumps({k: v for k, v in m.items() if not k.startswith('_')}))
    m['_warianty'] = var_df
    return df, m


def hist_table(df):
    """Wyniki testu historycznego per typ+wejscie i kierunek (tylko setupy po filtrach)."""
    rows = []
    if not len(df):
        return pd.DataFrame()
    ok = df[df.filtr.fillna('') == '']
    for (tv, side), d in ok.groupby(['typ_wersja', 'side']):
        st = stats(d)
        if not st.get('transakcje'):
            continue
        lq = liq_compare(d)
        rows.append({'typ': tv, 'side': side, 'transakcje': st.get('transakcje', 0), 'sredni_R': st.get('sredni_R'),
                     'losowe_R': st.get('losowe_R'), 'sredni_edge_R': st.get('sredni_edge_R'), 't_edge': st.get('t_edge'),
                     'skutecznosc_pct': st.get('skutecznosc_pct'), 'zdjecia': st.get('zdjecia'), **lq})
    return pd.DataFrame(rows)


def liq_compare(d):
    """Te same transakcje: zwykly TP1 vs TP na klastrze likwidacji (tylko gdy klaster byl dostepny i oba rozstrzygniete)."""
    if 'r_liq' not in d.columns:
        return {'liq_n': 0, 'liq_R_tp1': None, 'liq_R_klaster': None, 'liq_t_roznicy': None}
    x = d[d.status.isin(FINAL) & d.status_liq.isin(FINAL)].copy()
    if not len(x):
        return {'liq_n': 0, 'liq_R_tp1': None, 'liq_R_klaster': None, 'liq_t_roznicy': None}
    x['diff'] = pd.to_numeric(x.r_liq, errors='coerce') - x.r
    per = x.groupby('snapshot_utc')['diff'].mean()
    t = per.mean() / per.std() * math.sqrt(len(per)) if len(per) > 2 and per.std() > 0 else None
    return {'liq_n': int(len(x)), 'liq_R_tp1': rnd(x.r.mean(), 3), 'liq_R_klaster': rnd(pd.to_numeric(x.r_liq).mean(), 3),
            'liq_t_roznicy': rnd(t, 2)}


# ============================== ARCHIWUM I BRAMKA ==============================
FINAL = ('TP1', 'SL', 'timeout', 'BE', 'TRAIL')   # BE/TRAIL = reszta po 1R zamknieta stopem na wejsciu / trailing


def stats(df):
    t = df[df.status.isin(FINAL)]
    if not len(t):
        return {'transakcje': 0}
    per = t.groupby('snapshot_utc').r.mean()
    tstat = per.mean() / per.std() * math.sqrt(len(per)) if len(per) > 2 and per.std() > 0 else None
    edge = pd.to_numeric(t['edge_R'], errors='coerce') if 'edge_R' in t.columns else pd.Series(dtype=float)
    te = t.assign(_e=edge)[edge.notna()]
    pe = te.groupby('snapshot_utc')._e.mean()
    t_edge = pe.mean() / pe.std() * math.sqrt(len(pe)) if len(pe) > 2 and pe.std() > 0 else None
    return {'sredni_edge_R': rnd(edge.mean(), 3) if edge.notna().any() else None, 't_edge': rnd(t_edge, 2),
            'losowe_R': rnd(pd.to_numeric(te['control_R'], errors='coerce').mean(), 3) if len(te) else None,'sygnaly': int(len(df)), 'transakcje': int(len(t)), 'TP1': int((t.status == 'TP1').sum()),
            'SL': int((t.status == 'SL').sum()), 'timeout': int((t.status == 'timeout').sum()),
            'skutecznosc_pct': rnd((t.r > 0).mean() * 100, 1), 'sredni_R': rnd(t.r.mean(), 3),
            'suma_R': rnd(t.r.sum(), 2), 'zdjecia': int(len(per)), 't': rnd(tstat, 2),
            'wypelnienie_pct': rnd(df.filled.astype(str).str.lower().isin(['true', '1', '1.0']).mean() * 100, 1)}


def update_archive(bt, run_utc, drive_ok, now_ms):
    df = pd.DataFrame(bt)
    if len(df):
        df['run_utc'] = run_utc
        df['version'] = VERSION
    path = Path(DRIVE_DIR) / ARCHIVE_NAME
    if drive_ok and path.exists():
        try:
            old = pd.read_csv(path)
            df = pd.concat([old, df], ignore_index=True)
        except Exception as exc:
            print(f'Archiwum: nie udalo sie wczytac ({exc}) - zaczynam od nowa')
    if not len(df):
        return df
    df = df.sort_values('run_utc', kind='stable')
    df['zone_ref'] = df['zone_ref'].fillna(df['entry']) if 'zone_ref' in df.columns else df['entry']
    df = df.drop_duplicates(subset=['snapshot_utc', 'symbol', 'typ_wersja', 'side', 'zone_ref'], keep='last')
    # ta sama strefa z innego zdjecia (przesuniecie okna testu miedzy uruchomieniami) liczy sie raz - od najwczesniejszego
    df = df.sort_values('snapshot_utc', kind='stable').drop_duplicates(subset=['symbol', 'typ_wersja', 'side', 'zone_ref'], keep='first')
    cutoff = snap_iso(now_ms - BT_ARCHIVE_DAYS * DAY_MS)
    df = df[df.snapshot_utc >= cutoff].sort_values(['snapshot_utc', 'symbol']).reset_index(drop=True)
    if drive_ok:
        path.parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(path, index=False)
    return df


def gate_table(arch, now_ms):
    """V6.15: bramka osobno dla typu+wejscia+kierunku; nie pozwala, by dobre SHORT maskowaly zle LONG.
    DO GRY wymaga jednoczesnie dodatniego wyniku po kosztach, przewagi nad kontrola, t>=2,
    odpowiedniej liczby transakcji i wielu zdjec rynku. Statystyka jest konserwatywnym filtrem,
    nie gwarancja przewagi; transakcje z kolejnych zdjec moga byc skorelowane."""
    rows = []
    since = max(GATE_FROM_UTC, snap_iso(now_ms - GATE_WINDOW_DAYS * DAY_MS))
    combos = [(t, f'{t}.v{v}') for t, v in SETUP_TYPES.items()]
    if MARKET_ENTRIES:
        combos += [(t, f'{t}.v{v}.M') for t, v in SETUP_TYPES.items()]
    for typ, tv in combos:
        for side in ('LONG', 'SHORT'):
            d = arch[(arch.typ_wersja == tv) & (arch.side == side) & (arch.snapshot_utc >= since)] if len(arch) else arch
            ok = d[d.filtr.fillna('') == ''] if len(d) else d
            st = stats(ok) if len(ok) else {'transakcje': 0, 'zdjecia': 0}
            st_all = stats(d) if len(d) else {'transakcje': 0}
            n = int(st.get('transakcje', 0) or 0)
            ns = int(st.get('zdjecia', 0) or 0)
            avg_r = st.get('sredni_R')
            edge = st.get('sredni_edge_R')
            t_edge_snap = st.get('t_edge')
            # Korekta V6.15: kolejne zrzuty co 2h są skorelowane. Istotność edge liczymy
            # na średnich dziennych, aby nie udawać, że każdy zrzut jest niezależną próbą.
            t_edge_day, days = None, 0
            if len(ok) and 'edge_R' in ok.columns:
                e = ok[['snapshot_utc', 'edge_R']].copy()
                e['edge_R'] = pd.to_numeric(e['edge_R'], errors='coerce')
                e = e[e.edge_R.notna()]
                e['_day'] = e.snapshot_utc.astype(str).str[:10]
                daily = e.groupby('_day').edge_R.mean()
                days = int(len(daily))
                if days >= 3 and daily.std(ddof=1) > 0:
                    t_edge_day = float(daily.mean() / daily.std(ddof=1) * math.sqrt(days))
                    t_edge_day = rnd(t_edge_day, 2)
            active = (n >= GATE_MIN_TRADES and ns >= GATE_MIN_SNAPSHOTS and days >= GATE_MIN_DAYS
                      and avg_r is not None and avg_r >= GATE_MIN_AVG_R
                      and edge is not None and edge >= GATE_MIN_EDGE_R
                      and t_edge_day is not None and t_edge_day >= GATE_MIN_T)
            rows.append({'typ': tv, 'side': side, 'status': 'DO GRY' if active else 'OBSERWACJA', 'od': since,
                         'transakcje': n, 'sredni_R': avg_r, 'sredni_edge_R': edge,
                         't_edge_zrzuty': t_edge_snap, 't_edge_dzienny': t_edge_day,
                         't': st.get('t'), 'skutecznosc_pct': st.get('skutecznosc_pct'), 'zdjecia': ns,
                         'dni_edge': days, 'min_zdjec': GATE_MIN_SNAPSHOTS, 'min_dni': GATE_MIN_DAYS,
                         'brakuje_transakcji': max(0, GATE_MIN_TRADES - n),
                         'brakuje_zdjec': max(0, GATE_MIN_SNAPSHOTS - ns),
                         'brakuje_dni': max(0, GATE_MIN_DAYS - days),
                         'bez_filtrow_transakcje': st_all.get('transakcje', 0),
                         'bez_filtrow_sredni_R': st_all.get('sredni_R'), 'bez_filtrow_t': st_all.get('t')})
    return pd.DataFrame(rows)


def bt_summary(df):
    if not len(df):
        return {}
    out = {'WSZYSTKIE (po filtrach)': stats(df[df.filtr.fillna('') == ''])}
    out['WSZYSTKIE (bez filtrow)'] = stats(df)
    for typ in sorted(df.setup_type.unique()):
        d = df[df.setup_type == typ]
        out[f'{typ} po filtrach'] = stats(d[d.filtr.fillna('') == ''])
        out[f'{typ} odrzucone'] = stats(d[d.filtr.fillna('') != ''])
    for side in ('LONG', 'SHORT'):
        d = df[(df.side == side) & (df.filtr.fillna('') == '')]
        out[f'{side} po filtrach'] = stats(d)
    for mode in ('LIMIT', 'MARKET'):
        d = df[(df.entry_type == mode) & (df.filtr.fillna('') == '')]
        if len(d):
            out[f'{mode} po filtrach'] = stats(d)
    for flt in ('koszt', 'poscig', 'squeeze', 'anomalia_wolumenu', 'przeciw_trendowi', 'azja_short', 'short_godziny', 'fvg4h'):
        d = df[df.filtr.fillna('').str.contains(flt)]
        if len(d):
            out[f'odrzucone: {flt}'] = stats(d)
    return out


# ============================== DRIVE ==============================
def candle_file(sym):
    return Path(DRIVE_DIR) / 'candles_v63' / f'{sym}.npz'


def merge_candles(old, new):
    """Laczy swiece po czasie otwarcia; przy duplikatach wygrywa nowsza wersja."""
    if old is None or not len(old):
        return new
    if new is None or not len(new):
        return old
    a = np.vstack([old, new])
    _, idx = np.unique(a[::-1, T], return_index=True)   # pierwsze wystapienie od konca = najnowsza wersja
    return a[::-1][idx]


_arch_cache = {}


def load_candle_archive(drive_ok, syms=None):
    """V6.8: wczytuje archiwum swiec raz (do pobierania przyrostowego i do sklejenia). -> {sym: {tf: (pierwsza, ostatnia)}}"""
    known = {}
    if not (drive_ok and SAVE_CANDLES):
        return known
    d = Path(DRIVE_DIR) / 'candles_v63'
    if not d.exists():
        return known

    def one(p):
        try:
            with np.load(p) as z:
                return p.stem, {k: z[k] for k in z.files}
        except Exception:
            return p.stem, None
    files = [p for p in d.glob('*.npz') if not p.stem.endswith('.tmp') and (syms is None or p.stem in syms)]
    with ThreadPoolExecutor(max_workers=8) as ex:
        for sym, arrs in ex.map(one, files):
            if arrs:
                _arch_cache[sym] = arrs
                known[sym] = {tf: (int(a[0, T]), int(a[-1, T])) for tf, a in arrs.items() if len(a)}
    return known


FUNDING_CACHE = 'funding_cache_v69.json'


def load_funding_cache(drive_ok):
    """V6.9: historia fundingu z poprzednich uruchomien (Drive)."""
    p = Path(DRIVE_DIR) / FUNDING_CACHE
    if not drive_ok or not p.exists():
        return {}
    try:
        x = json.loads(p.read_text())
        return x if isinstance(x, dict) else {}
    except Exception:
        return {}


def save_funding_cache(coins, old, drive_ok):
    if not drive_ok:
        return
    new = dict(old or {})
    for sym, d in coins.items():
        h = d.get('funding_history')
        if isinstance(h, list) and len(h) >= 8:
            new[sym] = h
    try:
        (Path(DRIVE_DIR) / FUNDING_CACHE).write_text(json.dumps(new, separators=(',', ':')))
    except Exception as exc:
        print(f'Pamiec fundingu: blad zapisu ({str(exc)[:100]})')


def candles_to_rows(a, tf):
    """Macierz swiec -> wiersze jak z API (do coins/*.json, zeby REPLAY mial pelne okno)."""
    ms = INTERVAL_MS[tf]
    return [[int(r[T]), float(r[O]), float(r[H]), float(r[L]), float(r[C]), float(r[V]), int(r[T]) + ms - 1,
             float(r[QV]), 0, float(r[TB])] for r in a]


def gaps_qc(P):
    """V6.8: brakujace swiece w oknie analizy (WIN) -> {tf: liczba luk}."""
    out = {}
    for tf, a in P['arr'].items():
        w = a[-WIN.get(tf, 400):]
        if len(w) > 2:
            n = int((np.diff(w[:, T]) > INTERVAL_MS[tf] * 1.5).sum())
            if n:
                out[tf] = n
    return out


def extend_with_archive(PP, drive_ok, now_ms):
    """Dokleja swiece z archiwum na Drive (dluzsza historia dla testu) i zapisuje archiwum z nowymi swiecami."""
    if not (drive_ok and SAVE_CANDLES):
        return 0
    keep = {'5m': CANDLE_KEEP_DAYS, '15m': CANDLE_KEEP_DAYS, '1h': CANDLE_KEEP_DAYS + HIST_WARMUP_D['1h'],
            '4h': max(CANDLE_KEEP_DAYS + HIST_WARMUP_D['4h'], 90)}   # V6.9: 4h >= 500 swiec (pobieranie przyrostowe)
    (Path(DRIVE_DIR) / 'candles_v63').mkdir(parents=True, exist_ok=True)
    n = 0
    bledy = []

    def one(item):
        sym, P = item
        p = candle_file(sym)
        old = _arch_cache.get(sym) or {}
        if not old and p.exists():
            try:
                with np.load(p) as z:
                    old = {k: z[k] for k in z.files}
            except Exception:
                old = {}
        merged = {}
        for tf in ('5m', '15m', '1h', '4h'):
            m = merge_candles(old.get(tf), P['arr'].get(tf))
            if m is not None and len(m):
                m = m[m[:, T] >= now_ms - keep[tf] * DAY_MS]
                merged[tf] = m
                P['arr'][tf] = m
                P['_z_archiwum'] = True
        if merged:
            try:
                tmp = p.with_name(p.stem + '.tmp.npz')
                np.savez_compressed(tmp, **merged)
                tmp.replace(p)
            except Exception as exc:                 # V6.9: dane i tak sklejone w pamieci
                bledy.append(f'{sym}: {str(exc)[:60]}')
        return 1
    with ThreadPoolExecutor(max_workers=8) as ex:
        for k in ex.map(one, PP.items()):
            n += k
    if bledy:
        print(f'Archiwum swiec: blad zapisu dla {len(bledy)} kontraktow (np. {bledy[0]}) - analiza na danych w pamieci')
    return n


def mount_drive():
    try:
        drive.mount('/content/drive')
        Path(DRIVE_DIR).mkdir(parents=True, exist_ok=True)
        return True
    except Exception as exc:
        print(f'Google Drive niedostepny ({exc}) - bez archiwum i historii OI')
        return False


def load_oi_history(drive_ok):
    p = Path(DRIVE_DIR) / OI_HISTORY_NAME
    if drive_ok and p.exists():
        try:
            return json.loads(p.read_text())
        except Exception:
            pass
    return {}


def save_oi_history(hist, drive_ok, now_ms):
    if not drive_ok:
        return
    cutoff = now_ms - OI_HISTORY_KEEP_DAYS * DAY_MS
    hist = {k: [x for x in v if x[0] >= cutoff] for k, v in hist.items()}
    (Path(DRIVE_DIR) / OI_HISTORY_NAME).write_text(json.dumps(hist))


# ============================== GLOWNA ==============================
def write_csv(name, rows, first=()):
    df = pd.DataFrame(rows)
    if len(df):
        cols = [c for c in first if c in df.columns] + [c for c in df.columns if c not in first]
        df = df[cols]
    df.to_csv(OUT / name, index=False)
    return df


SETUP_FIRST = ['ocena', 'ocena_pkt', 'status_bramki', 'symbol', 'side', 'setup_type', 'entry_type', 'price', 'entry', 'dist_entry_pct', 'sl',
               'tp1', 'tp2', 'rr_tp1_net', 'risk_pct', 'dzwignia', 'dzwignia_bezp', 'dzwignia_pref', 'likwidacja', 'likw_vs_sl', 'wartosc_usdt', 'depozyt_usdt', 'depozyt_pct', 'ilosc', 'strata_sl_usdt', 'zysk_tp1_usdt', 'dzwignia_uwagi', 'cost_R', 'sl_atr', 'wait_h', 'score_R', 'hist_n', 'hist_sr_R',
               'hist_edge_R', 'hist_t_edge', 'hist_zrodlo', 'tp_liq', 'rr_tp_liq', 'liq_tp_share', 'liq_za_sl_share',
               'trigger', 'cancel_if', 'wazne_do_pl', 'retesty_15m', 'ruch24_kier', 'kontra_24h', 'pogon_24h', 'wolumen_skok', 'filtr', 'tradable', 'not_tradable_reason', 'typ_transakcje', 'typ_sredni_R', 'typ_t']
TOP_FIRST = ['mode', 'rank', 'status_bramki', 'symbol', 'side', 'setup_type', 'price', 'entry', 'dist_entry_pct', 'sl', 'tp1',
             'tp2', 'rr_tp1_net', 'risk_pct', 'dzwignia', 'dzwignia_bezp', 'dzwignia_pref', 'likwidacja', 'likw_vs_sl', 'wartosc_usdt', 'depozyt_usdt', 'depozyt_pct', 'ilosc', 'strata_sl_usdt', 'zysk_tp1_usdt', 'dzwignia_uwagi', 'cost_R', 'score_R', 'dlaczego', 'trigger', 'cancel_if']
RANK_FIRST = ['ocena', 'ocena_pkt', 'do_handlu', 'w_limicie_ryzyka', 'warunki', 'entry_type', 'symbol', 'vol_rank', 'asset_type', 'side', 'setup_type', 'price', 'entry', 'dist_entry_pct', 'sl', 'tp1',
              'rr_tp1_net', 'risk_pct', 'cost_R', 'dzwignia', 'dzwignia_bezp', 'filtr', 'przegrzanie', 'zgodnosc_momentum', 'ocena_powody', 'trigger', 'cancel_if', 'wazne_do_pl', 'retesty_15m']
POS_FIRST = ['symbol', 'side', 'akcja', 'zamknij_pct', 'nowy_sl', 'zamknij_najpozniej_pl', 'price', 'entry', 'sl', 'tp', 'R_teraz', 'MFE_R', 'pnl_usdt',
             'powody', 'uwagi']
BT_FIRST = ['snapshot_utc', 'symbol', 'setup_type', 'typ_wersja', 'side', 'entry_type', 'entry', 'sl', 'tp1',
            'rr_tp1_net', 'filtr', 'status', 'filled', 'r', 'control_R', 'edge_R', 'tp_liq', 'status_liq', 'r_liq', 'mfe_R', 'mae_R', 'fill_utc', 'exit_utc', 'bars_held']



# ================= V6.16: DIAGNOSTYKA (BEZ ZMIANY BRAMKI) =================
# Zasady z badan: nowe filtry sa najpierw oceniane out-of-sample, nie dopuszczaja
# samodzielnie transakcji. Poziomy ATR sa sugestiami, a nie zamiana zlecen SL/TP.
V616_MIN_OOS_N = 30
V616_MIN_OOS_DAYS = 5


def v616_market_context(A, f, side=None):
    """Kontekst z zamknietych swiec, bez uzywania przyszlych obserwacji."""
    a5, a15 = A['5m'], A['15m']
    def swings(a, lookback=12):
        if len(a) < lookback + 2:
            return 'BRAK_DANYCH'
        old, new = a[-2*lookback:-lookback], a[-lookback:]
        if len(old) < lookback:
            return 'BRAK_DANYCH'
        highs = (float(np.max(old[:, H])), float(np.max(new[:, H])))
        lows = (float(np.min(old[:, L])), float(np.min(new[:, L])))
        if highs[1] > highs[0] and lows[1] > lows[0]: return 'HH_HL'
        if highs[1] < highs[0] and lows[1] < lows[0]: return 'LH_LL'
        return 'MIESZANA'
    st5, st15 = swings(a5), swings(a15, 8)
    a = a5
    vols = a[-25:-1, V] if len(a) >= 25 else a[:-1, V]
    rv = float(a[-1, V] / np.median(vols)) if len(vols) and np.median(vols) > 0 else None
    high_prev = float(np.max(a[-13:-1, H])) if len(a) >= 13 else None
    low_prev = float(np.min(a[-13:-1, L])) if len(a) >= 13 else None
    close = float(a[-1, C])
    bos_up = high_prev is not None and close > high_prev
    bos_dn = low_prev is not None and close < low_prev
    reg = f.get('regime_1h')
    ar = f.get('atr_rank_1h')
    high_vol = ar is not None and np.isfinite(ar) and ar >= 0.85
    if high_vol: regime_name = 'WYSOKA_ZMIENNOSC'
    elif f.get('trend_1h') == 1 and reg != 'CHOP': regime_name = 'TREND_WZROSTOWY'
    elif f.get('trend_1h') == -1 and reg != 'CHOP': regime_name = 'TREND_SPADKOWY'
    else: regime_name = 'KONSOLIDACJA_LUB_NIEJASNY'
    agree = ((st5 == st15 == 'HH_HL') if side == 'LONG' else
             (st5 == st15 == 'LH_LL') if side == 'SHORT' else False)
    confirmed = bool(agree and rv is not None and rv >= 1.2 and (bos_up if side == 'LONG' else bos_dn))
    return {'v616_rezim': regime_name, 'v616_struktura_5m': st5,
            'v616_struktura_15m': st15, 'v616_rvol_5m': rnd(rv, 3),
            'v616_bos_5m': 'UP' if bos_up else 'DOWN' if bos_dn else 'BRAK',
            'v616_potwierdzenie': confirmed}


def v616_target_diagnostics(r, A, f):
    """Pomocnicze SL/TP oparte o 15m i ATR; NIE zastępuja bazowych zlecen."""
    side = r.get('side')
    if side not in ('LONG','SHORT'): return {}
    a = A['15m']
    if len(a) < 20: return {'v616_cel_status': 'BRAK_SWIEC'}
    entry = float(r['entry'])
    av = float(f.get('atr15') or 0)
    if av <= 0: return {'v616_cel_status': 'BRAK_ATR'}
    recent = a[-20:]
    stop = (float(np.min(recent[:, L])) - 0.25*av) if side == 'LONG' else (float(np.max(recent[:, H])) + 0.25*av)
    risk = (entry-stop) if side == 'LONG' else (stop-entry)
    if risk <= 0: return {'v616_cel_status': 'SL_PO_ZLEJ_STRONIE'}
    # Najblizszy lokalny ekstremum z poprzednich 80 zamknietych swiec 15m.
    older = a[-81:-1] if len(a) >= 81 else a[:-1]
    levels = older[:, H] if side == 'LONG' else older[:, L]
    targets = [float(x) for x in levels if (x > entry if side == 'LONG' else x < entry)]
    if not targets: return {'v616_cel_status': 'BRAK_STRUKTURALNEGO_TP', 'v616_sl_atr': rnd(stop, 8)}
    tp = min(targets) if side == 'LONG' else max(targets)
    reward = (tp-entry) if side == 'LONG' else (entry-tp)
    return {'v616_cel_status': 'KANDYDAT_NIEZWERYFIKOWANY', 'v616_sl_atr': rnd(stop, 8),
            'v616_tp1_struktura': rnd(tp, 8), 'v616_rr_brutto': rnd(reward/risk, 3),
            'v616_rr_min_1_5': bool(reward/risk >= 1.5)}


def v616_oos_report(df):
    """Chronologiczny holdout, po kosztach z backtestu, bez ponownej optymalizacji."""
    if df is None or not len(df): return []
    d = df.copy()
    if 'snapshot_utc' not in d or 'r' not in d: return []
    d['time'] = pd.to_datetime(d['snapshot_utc'], utc=True, errors='coerce')
    d['result'] = pd.to_numeric(d['r'], errors='coerce')
    if 'status' in d:
        d = d[d['status'].isin(FINAL)]
    d = d.dropna(subset=['time','result'])
    if not len(d): return []
    d['day'] = d['time'].dt.date
    d['type'] = d['typ_wersja'].astype(str) if 'typ_wersja' in d else 'ALL'
    d['direction'] = d['side'].astype(str) if 'side' in d else 'ALL'
    out=[]
    for (typ,side), group in d.groupby(['type','direction']):
        days=sorted(group['day'].unique())
        if len(days) < 2: continue
        cut=days[max(1, int(len(days)*0.7)) - 1]
        train=group[group['day'] <= cut]
        test=group[group['day'] > cut]
        edge=pd.to_numeric(test['edge_R'],errors='coerce').dropna() if 'edge_R' in test else pd.Series(dtype=float)
        qualified = len(test)>=V616_MIN_OOS_N and test['day'].nunique()>=V616_MIN_OOS_DAYS
        out.append({'typ':typ,'side':side,'train_n':len(train),'train_avg_R':rnd(train['result'].mean(),3),
                    'oos_n':len(test),'oos_days':int(test['day'].nunique()),
                    'oos_avg_R':rnd(test['result'].mean(),3) if len(test) else None,
                    'oos_edge_R':rnd(edge.mean(),3) if len(edge) else None,
                    'oos_status':'DO_OCENY' if qualified else 'ZA_MALO_DANYCH',
                    'oos_potwierdza':bool(qualified and len(edge)>=V616_MIN_OOS_N and test['result'].mean()>0 and edge.mean()>0)})
    return out

# ================= KONIEC DODATKU V6.16 =================

# ================= V6.17: WALIDOWANA DIAGNOSTYKA (BEZ OMIJANIA BRAMKI) =================
V617_MIN_SWING_ATR = 0.55
V617_MIN_TARGET_RR = 1.5

def v617_closed(a, now_ms, tf):
    """Usun swiece niezakończone wedlug czasu serwera, niezaleznie od flag API."""
    if a is None or not len(a): return np.empty((0, 6))
    return a[np.isfinite(a[:, T]) & (a[:, T] + INTERVAL_MS[tf] <= now_ms)]


def v617_pivots(a, atr, radius=2, min_sep=3):
    """Potwierdzone swingi: po prawej musza byc radius zamkniete swiece."""
    if len(a) < radius*2+5 or not np.isfinite(atr) or atr <= 0: return [], []
    hi, lo = [], []
    for i in range(radius, len(a)-radius):
        h, l = float(a[i,H]), float(a[i,L])
        if h >= max(a[i-radius:i+radius+1,H]) and h - min(a[max(0,i-5):i+1,L]) >= atr*V617_MIN_SWING_ATR:
            if not hi or i-hi[-1][0]>=min_sep: hi.append((i,h))
        if l <= min(a[i-radius:i+radius+1,L]) and max(a[max(0,i-5):i+1,H]) - l >= atr*V617_MIN_SWING_ATR:
            if not lo or i-lo[-1][0]>=min_sep: lo.append((i,l))
    return hi, lo


def v617_regime(A, f, side, now_ms):
    a5=v617_closed(A.get('5m'),now_ms,'5m')
    a15=v617_closed(A.get('15m'),now_ms,'15m')
    a1=v617_closed(A.get('1h'),now_ms,'1h')
    out={'v617_status':'BRAK_DANYCH','v617_struktura':'NIEZNANA','v617_korekta_5m':False,
         'v617_potwierdzenie':False,'v617_powod':'brak zamknietych swiec'}
    if min(len(a5),len(a15),len(a1)) < 25: return out
    atr=float(f.get('atr15') or np.nan)
    if not np.isfinite(atr) or atr<=0: return out
    h15,l15=v617_pivots(a15[-120:],atr)
    h5,l5=v617_pivots(a5[-120:],max(atr/3,1e-12))
    def structure(h,l):
        if len(h)<2 or len(l)<2: return 'NIEJASNA'
        if h[-1][1]>h[-2][1] and l[-1][1]>l[-2][1]: return 'HH_HL'
        if h[-1][1]<h[-2][1] and l[-1][1]<l[-2][1]: return 'LH_LL'
        return 'MIESZANA'
    st15,st5=structure(h15,l15),structure(h5,l5)
    trend = 'LH_LL' if side=='SHORT' else 'HH_HL'
    opposite = 'HH_HL' if side=='SHORT' else 'LH_LL'
    trend1=f.get('trend_1h')
    agree1=trend1==(-1 if side=='SHORT' else 1)
    correction=(st15==trend and st5==opposite)
    # Potwierdzenie nie wymaga identycznego trendu 5m; wymaga zakonczenia korekty
    recent=a5[-7:]
    prev=a5[-19:-7]
    if side=='SHORT':
        trigger=float(recent[-1,C])<float(np.min(prev[:,L]))
    else:
        trigger=float(recent[-1,C])>float(np.max(prev[:,H]))
    volbase=np.median(a5[-26:-1,V])
    rvol=float(a5[-1,V]/volbase) if volbase>0 else None
    # V6.22: audyt skladowych; nie zmieniamy warunkow potwierdzenia.
    checks={'struktura_15m':st15==trend,'trend_1h':agree1,
            'wybicie_5m':trigger,'rvol_5m':rvol is not None and rvol>=1.2}
    confirmed=all(checks.values())
    missing=[name for name,passed in checks.items() if not passed]
    out.update(v622_struktura_15m_ok=checks['struktura_15m'],
               v622_trend_1h_ok=checks['trend_1h'],
               v622_wybicie_5m_ok=checks['wybicie_5m'],
               v622_rvol_5m_ok=checks['rvol_5m'],
               v622_brakujace_warunki=' | '.join(missing),
               v622_pivot_high_15m=len(h15),v622_pivot_low_15m=len(l15),
               v622_struktura_15m=st15,v622_struktura_5m=st5,
               v622_rvol_5m=rnd(rvol,3),
               v622_btc_trend_1h=trend1,
               v617_status='OK',v617_struktura=st15,v617_struktura_5m=st5,
               v617_korekta_5m=correction,v617_potwierdzenie=confirmed,
               v617_rvol=rnd(rvol,3),v617_powod=('potwierdzony impuls' if confirmed else
               'korekta 5m przeciw trendowi 15m' if correction else 'brak potwierdzenia impulsu'))
    return out


def v617_levels(r,A,f,now_ms):
    """Strefy z potwierdzonych pivotow 15m i 1h; zadnych przypadkowych knotow jako TP."""
    side=r.get('side'); entry=float(r.get('entry') or 0)
    out={'v617_poziomy_status':'BRAK_DANYCH','v617_sl_sug':None,'v617_tp1_sug':None,
         'v617_rr_brutto':None,'v617_rr_ok':False}
    if side not in ('LONG','SHORT') or entry<=0:return out
    a15=v617_closed(A.get('15m'),now_ms,'15m')
    a1=v617_closed(A.get('1h'),now_ms,'1h')
    atr=float(f.get('atr15') or np.nan)
    if len(a15)<35 or len(a1)<25 or not np.isfinite(atr) or atr<=0:return out
    hi15,lo15=v617_pivots(a15[-160:],atr)
    hi1,lo1=v617_pivots(a1[-100:],atr*3)
    if side=='LONG':
        stops=[v for _,v in lo15[-6:]+lo1[-4:] if v<entry]
        targets=[v for _,v in hi15+hi1 if v>entry+atr*.35]
        stop=max(stops)-.3*atr if stops else float(np.min(a15[-20:,L]))-.3*atr
    else:
        stops=[v for _,v in hi15[-6:]+hi1[-4:] if v>entry]
        targets=[v for _,v in lo15+lo1 if v<entry-atr*.35]
        stop=min(stops)+.3*atr if stops else float(np.max(a15[-20:,H]))+.3*atr
    risk=abs(entry-stop)
    if risk<atr*.35 or risk>atr*8:return {**out,'v617_poziomy_status':'RYZYKO_NIEODPOWIEDNIE'}
    # Grupuj poziomy w strefy o szerokosci 0.35 ATR, wymagaj dwoch dotkniec
    targets.sort(reverse=(side=='SHORT'))
    clusters=[]
    for value in targets:
        hit=next((c for c in clusters if abs(c[0]-value)<=atr*.35),None)
        if hit is None:clusters.append([value,1])
        else:
            hit[0]=(hit[0]*hit[1]+value)/(hit[1]+1);hit[1]+=1
    valid=[]
    for level,n in clusters:
        reward=(level-entry) if side=='LONG' else (entry-level)
        if n>=2 and reward>0 and reward/risk>=V617_MIN_TARGET_RR:valid.append((reward,level,n))
    out['v617_sl_sug']=rnd(stop,8)
    if not valid:
        out['v617_poziomy_status']='BRAK_POTWIERDZONEGO_TP_RR'
        return out
    reward,tp,n=min(valid)
    out.update(v617_poziomy_status='KANDYDAT_DIAGNOSTYCZNY',v617_tp1_sug=rnd(tp,8),
               v617_rr_brutto=rnd(reward/risk,3),v617_rr_ok=True,v617_liczba_testow=n)
    return out


def v617_walkforward(df):
    """Rozlaczne chronologiczne okna testowe; bez strojenia parametrow na tescie."""
    if df is None or not len(df) or not {'snapshot_utc','r'}.issubset(df.columns):return []
    d=df.copy()
    d['date']=pd.to_datetime(d.snapshot_utc,utc=True,errors='coerce')
    d['r']=pd.to_numeric(d.r,errors='coerce')
    if 'status' in d:d=d[d.status.isin(FINAL)]
    d=d.dropna(subset=['date','r'])
    if not len(d):return []
    d['day']=d.date.dt.floor('D')
    for key in ('typ_wersja','side','entry_type'):
        if key not in d:d[key]='BRAK'
    out=[]
    for keys,g in d.groupby(['typ_wersja','side','entry_type']):
        days=sorted(g.day.unique())
        if len(days)<14:
            out.append(dict(zip(('typ','side','entry_type'),keys),status='ZA_MALO_DNI',dni=len(days),foldy=0))
            continue
        test_days=max(3,len(days)//5)
        splits=[]
        for end in range(max(7,len(days)//2),len(days)-test_days+1,test_days):
            train=g[g.day.isin(days[:end])]
            test=g[g.day.isin(days[end:end+test_days])]
            if len(train)<30 or len(test)<10:continue
            edge=pd.to_numeric(test.get('edge_R',pd.Series(dtype=float)),errors='coerce').dropna()
            splits.append({'train_n':len(train),'test_n':len(test),'test_avg_R':float(test.r.mean()),
                           'test_edge_R':float(edge.mean()) if len(edge) else None})
        result=dict(zip(('typ','side','entry_type'),keys))
        result.update(status='DIAGNOSTYKA' if len(splits)>=2 else 'ZA_MALO_FOLDOW',
                      dni=len(days),foldy=len(splits),oos_transakcje=sum(x['test_n'] for x in splits),
                      oos_sredni_R=rnd(np.average([x['test_avg_R'] for x in splits],weights=[x['test_n'] for x in splits]),3) if splits else None,
                      dodatnie_foldy=sum(x['test_avg_R']>0 for x in splits),
                      foldy_json=json.dumps(splits,ensure_ascii=False))
        out.append(result)
    return out

# ================= KONIEC V6.17 =================

# ================= V6.18: DIAGNOSTYKA JAKOSCI I HISTORII =================
def v618_oi_audit(summary, oi_hist, now_ms):
    """OI: tylko kontrola danych, nigdy zamiana brakow na zero ani odblokowanie bramki."""
    out=[]
    for row in summary:
        sym=row.get('symbol')
        pts=[]
        for item in oi_hist.get(sym, []):
            try:
                t,v=float(item[0]),float(item[1])
                if 0 < v < float('inf') and t <= now_ms and t >= now_ms-30*86400000:
                    pts.append((t,v))
            except (ValueError,TypeError,IndexError):
                continue
        pts.sort()
        clean=[]
        for t,v in pts:
            if clean and t==clean[-1][0]:clean[-1]=(t,v)
            else:clean.append((t,v))
        oi=row.get('oi_usdt')
        qv=row.get('quote_vol_24h')
        status='OK'
        if not row.get('oi_wiarygodne',False):status='NIEWIARYGODNE'
        elif oi is None or float(oi)<=0:status='BRAK_OI'
        elif qv is None or float(qv)<=0:status='BRAK_WOLUMENU'
        elif len(clean)<2:status='BRAK_HISTORII'
        elif now_ms-clean[-1][0]>6*3600000:status='HISTORIA_NIEAKTUALNA'
        ratios=[]
        for a,b in zip(clean,clean[1:]):
            if b[0]-a[0] >= 15*60000:
                ratios.append(abs(b[1]/a[1]-1))
        if status=='OK' and ratios and max(ratios)>5:status='SKOK_OI_DO_WERYFIKACJI'
        out.append({'symbol':sym,'status_oi_v618':status,'oi_usdt':oi,
                    'oi_hist_punkty_30d':len(clean),'oi_hist_dni':round((clean[-1][0]-clean[0][0])/86400000,2) if len(clean)>1 else 0,
                    'oi_hist_wiek_min':round((now_ms-clean[-1][0])/60000,1) if clean else None,
                    'oi_do_wolumenu_24h':round(float(oi)/float(qv),3) if oi and qv else None,
                    'max_skok_oi_pct':round(max(ratios)*100,2) if ratios else None})
    return out


def v618_rejection_audit(live):
    """Jedna linia na setup i policzalne przyczyny odrzucenia; diagnostyka bez zmiany decyzji."""
    rows=[]
    for r in live:
        causes=[]
        if r.get('filtr'):causes.extend('filtr: '+v.strip() for v in str(r['filtr']).split(';') if v.strip())
        if not r.get('tradable',False):causes.append('nietradowalny: '+str(r.get('not_tradable_reason') or 'brak powodu'))
        if str(r.get('status_bramki'))!='DO GRY':causes.append('bramka: '+str(r.get('status_bramki') or 'brak'))
        if r.get('v617_status')!='OK':causes.append('struktura: '+str(r.get('v617_status') or 'brak'))
        if not r.get('v617_potwierdzenie'):causes.append('brak potwierdzenia struktury V6.17')
        if not r.get('v617_rr_ok'):causes.append('brak potwierdzonego TP/SL V6.17')
        if not r.get('w_limicie_ryzyka',False):causes.append('nie wybrano do limitu ryzyka')
        rows.append({'symbol':r.get('symbol'),'side':r.get('side'),'entry_type':r.get('entry_type'),
                     'setup_type':r.get('setup_type'),'ocena':r.get('ocena'),'do_handlu':r.get('do_handlu'),
                     'status_bramki':r.get('status_bramki'),'powody_v618':' | '.join(causes),
                     'liczba_powodow':len(causes),'v617_potwierdzenie':r.get('v617_potwierdzenie')})
    return rows


def v618_history_coverage(arch):
    """Ile faktycznie dni obejmuja grupy backtestu; bez sztucznego rozszerzania historii."""
    if arch is None or not len(arch) or 'snapshot_utc' not in arch:return []
    d=arch.copy()
    d['day']=pd.to_datetime(d['snapshot_utc'],utc=True,errors='coerce').dt.floor('D')
    for key in ('typ_wersja','side','entry_type'):
        if key not in d:d[key]='BRAK'
    out=[]
    for keys,g in d.dropna(subset=['day']).groupby(['typ_wersja','side','entry_type']):
        days=g['day'].nunique()
        out.append(dict(zip(('typ','side','entry_type'),keys),
                        dni=int(days),wiersze=len(g),
                        od_utc=str(g['day'].min()),do_utc=str(g['day'].max()),
                        dni_do_minimum=max(0,14-int(days)),
                        status='WYSTARCZA_NA_WSTEPNE_WF' if days>=14 else 'ZA_MALO_DNI'))
    return out

# ================= KONIEC V6.18 =================

# ================= V6.19: HISTORIA DRIVE + JAKOSC OI + WATCHLIST =================
# Zapis historyczny jest diagnostyczny; nie podnosi ocen i nie odblokowuje bramki.

def v619_drive_status(drive_ok, arch, oi_hist, now_ms):
    root = Path(DRIVE_DIR)
    paths = {'backtest_archive': root / ARCHIVE_NAME,
             'oi_history': root / OI_HISTORY_NAME,
             'candles': root / 'candles_v63'}
    rows = []
    for kind, path in paths.items():
        try:
            exists = bool(drive_ok and path.exists())
            size = path.stat().st_size if exists and path.is_file() else None
            rows.append({'zasob':kind,'drive_podlaczony':bool(drive_ok),'istnieje':exists,
                         'sciezka':str(path),'rozmiar_bajty':size,
                         'status':'OK' if exists else ('BRAK_DYSKU' if not drive_ok else 'BRAK_DANYCH')})
        except OSError as exc:
            rows.append({'zasob':kind,'drive_podlaczony':bool(drive_ok),'istnieje':False,
                         'sciezka':str(path),'status':'BLAD: '+str(exc)[:100]})
    days = 0
    if arch is not None and len(arch) and 'snapshot_utc' in arch:
        days = int(pd.to_datetime(arch['snapshot_utc'], utc=True, errors='coerce').dt.date.nunique())
    rows.append({'zasob':'dni_archiwum','drive_podlaczony':bool(drive_ok),'istnieje':bool(days),
                 'sciezka':'liczba unikalnych dni z archiwum','rozmiar_bajty':days,
                 'status':'ZA_MALO_DNI' if days < 14 else 'HISTORIA_ROSNIE'})
    return rows


def v619_oi_quality(oi_audit):
    rows = []
    for a in oi_audit:
        status = a.get('status_oi_v618') or 'BRAK'
        age = a.get('oi_hist_wiek_min')
        points = int(a.get('oi_hist_punkty_30d') or 0)
        jump = a.get('max_skok_oi_pct')
        reasons=[]
        if status != 'OK': reasons.append(status)
        if points < 3: reasons.append('mniej_niz_3_pomiary')
        if age is not None and float(age) > 360: reasons.append('starsze_niz_6h')
        if jump is not None and float(jump) > 500: reasons.append('skok_ponad_500pct')
        rows.append({'symbol':a.get('symbol'),'oi_status':status,
                     'oi_jakosc':'OK' if not reasons else 'NIE_POTWIERDZONE',
                     'powody':' | '.join(dict.fromkeys(reasons)),
                     'historia_punktow':points,'historia_dni':a.get('oi_hist_dni'),
                     'ostatni_pomiar_wiek_min':age,'maks_skok_pct':jump,
                     'uwaga':'OI diagnostyczne; brak nie jest zerem'})
    return rows


def v619_rejection_summary(reject_rows):
    from collections import Counter
    counter=Counter()
    by_side=Counter()
    for r in reject_rows:
        side=str(r.get('side') or 'BRAK')
        for cause in str(r.get('powody_v618') or '').split(' | '):
            if cause.strip():
                counter[cause.strip()] += 1
                by_side[(cause.strip(),side)] += 1
    return [{'powod':reason,'liczba':n,'long':by_side[(reason,'LONG')],
             'short':by_side[(reason,'SHORT')]} for reason,n in counter.most_common()]


def v619_watchlist(live, limit_per_side=12):
    # Lista obserwacyjna, NIE lista sygnalow do handlu.
    out=[]
    for side in ('LONG','SHORT'):
        candidates=[]
        for r in live:
            if r.get('side') != side: continue
            try:
                rr=float(r.get('rr_tp1_net') or 0)
                score=float(r.get('ocena_pkt') or 0)
                entry=float(r.get('entry'))
                sl=float(r.get('sl'))
                tp=float(r.get('tp1'))
                if not (math.isfinite(rr) and math.isfinite(entry) and math.isfinite(sl) and math.isfinite(tp)): continue
                if rr <= 0 or min(entry,sl,tp) <= 0: continue
            except (ValueError,TypeError): continue
            if r.get('do_handlu'): continue  # zatwierdzone setupy sa w top_setups, nie tutaj
            quality = (int(bool(r.get('v617_potwierdzenie'))),
                       int(bool(r.get('v617_rr_ok'))), score, rr)
            candidates.append((quality,r))
        candidates.sort(key=lambda item:item[0],reverse=True)
        for idx,(_,r) in enumerate(candidates[:limit_per_side],1):
            out.append({'status':'TYLKO_OBSERWACJA','rank':idx,'symbol':r.get('symbol'),
                        'side':side,'entry_type':r.get('entry_type'),'setup_type':r.get('setup_type'),
                        'price':r.get('price'),'entry':r.get('entry'),'sl':r.get('sl'),
                        'tp1':r.get('tp1'),'rr_tp1_net':r.get('rr_tp1_net'),
                        'ocena':r.get('ocena'),'status_bramki':r.get('status_bramki'),
                        'v617_potwierdzenie':r.get('v617_potwierdzenie'),
                        'v617_rr_ok':r.get('v617_rr_ok'),'filtr':r.get('filtr'),
                        'do_handlu':False,'uwaga':'NIE OTWIERAC na podstawie tej listy'})
    return out


def v619_save_drive_diagnostics(drive_ok, now_ms, rows):
    # Przy kazdym uruchomieniu zostaje w Drive ostatnia diagnostyka i historia dat.
    if not drive_ok: return 'BRAK_DYSKU'
    try:
        root=Path(DRIVE_DIR) / 'v619_diagnostics'
        root.mkdir(parents=True,exist_ok=True)
        stamp=datetime.fromtimestamp(now_ms/1000,timezone.utc).strftime('%Y%m%d_%H%M%S')
        target=root / f'run_{stamp}.json'
        temp=target.with_suffix('.tmp')
        temp.write_text(json.dumps(rows,ensure_ascii=False,default=str),encoding='utf-8')
        temp.replace(target)
        return str(target)
    except Exception as exc:
        print(f'V6.19: nie zapisano diagnostyki Drive: {str(exc)[:120]}')
        return 'BLAD_ZAPISU'





# ============================================================
# V6.20: WYŁĄCZNIE DIAGNOSTYKA HISTORYCZNA, BEZ ZMIANY BRAMKI
# ============================================================
def v620_diagnose(history, oi_quality=None):
    """Kohorty wyników R z ochroną przed look-ahead i małymi próbami.

    Snapshot i cechy są sprzed transakcji. Wiersze nierozstrzygnięte nie
    są zaliczane do wyników. OOS dzielone jest chronologicznie po dniach.
    Raport NIE zatwierdza transakcji ani nie dobiera automatycznie progów.
    """
    import numpy as np
    import pandas as pd
    out = {'groups': [], 'oos': [], 'filters': [], 'meta': {}}
    if history is None or not len(history):
        out['meta'] = {'status':'BRAK_HISTORII'}
        return out
    d = history.copy()
    required = {'r','snapshot_utc','typ_wersja','side'}
    if not required.issubset(d.columns):
        out['meta'] = {'status':'BRAK_KOLUMN', 'missing':sorted(required-set(d.columns))}
        return out
    d['r'] = pd.to_numeric(d['r'], errors='coerce')
    d['snapshot_dt'] = pd.to_datetime(d['snapshot_utc'], utc=True, errors='coerce')
    d = d.dropna(subset=['r','snapshot_dt']).copy()
    if 'filled' in d.columns:
        d = d[d['filled'].astype(str).str.lower().isin(['true','1','1.0'])].copy()
    d['day'] = d['snapshot_dt'].dt.strftime('%Y-%m-%d')
    d['hour_utc_v620'] = d['snapshot_dt'].dt.hour
    d['session_v620'] = pd.cut(d['hour_utc_v620'],[-1,6,13,20,23],labels=['AZJA','EUROPA','USA','NOC'],include_lowest=True).astype(str)
    for k in ('btc_trend_1h','btc_trend_4h','atr_rank_1h','atr_1h_pct','oi_chg_1h_bt'):
        if k in d.columns: d[k] = pd.to_numeric(d[k],errors='coerce')
    if 'btc_trend_1h' in d.columns:
        d['btc_trend_v620'] = d['btc_trend_1h'].map({1:'BTC_WZROST',-1:'BTC_SPADEK',0:'BTC_BOK'}).fillna('BRAK')
    else: d['btc_trend_v620']='BRAK'
    if 'regime_1h' not in d.columns: d['regime_1h']='BRAK'
    d['regime_1h']=d['regime_1h'].fillna('BRAK').astype(str)
    if 'atr_rank_1h' in d.columns:
        # Stałe pasma, a nie kwantyle liczone na przyszłych obserwacjach.
        v=d['atr_rank_1h']; d['zmiennosc_v620']=np.select([v<0.33,v<=0.67,v>0.67],['NISKA','SREDNIA','WYSOKA'],default='BRAK')
    else: d['zmiennosc_v620']='BRAK'
    if 'oi_chg_1h_bt' in d.columns:
        v=d['oi_chg_1h_bt']
        d['oi_v620']=np.select([v.isna(),v.abs()>100,v.abs()<=100],['BRAK','PODEJRZANY_SKOK','DOSTEPNY'],default='BRAK')
    else: d['oi_v620']='BRAK_HISTORYCZNEGO_OI'
    d['strategia_v620']=d['typ_wersja'].fillna('BRAK').astype(str)+' / '+d['side'].fillna('BRAK').astype(str)
    if 'entry_type' in d.columns: d['strategia_v620'] += ' / '+d['entry_type'].fillna('BRAK').astype(str)
    dims=['btc_trend_v620','regime_1h','zmiennosc_v620','session_v620','oi_v620']
    def stats(g):
        n=len(g); avg=float(g['r'].mean()); days=int(g['day'].nunique());
        return {'transakcje':n,'dni':days,'sredni_R':round(avg,4),
                'mediana_R':round(float(g['r'].median()),4),
                'winrate_pct':round(100*float((g['r']>0).mean()),2),
                'suma_R':round(float(g['r'].sum()),3),
                'status':'ZA_MALO_DANYCH' if n<30 or days<14 else 'DO_DALSZEJ_WALIDACJI'}
    for dim in dims:
        for (strategy, val),g in d.groupby(['strategia_v620',dim],dropna=False):
            out['groups'].append({'strategia':strategy,'wymiar':dim,'wartosc':str(val),**stats(g)})
    # Jedyny dopuszczalny podział: chronologiczny po unikalnych dniach.
    for strategy,g in d.groupby('strategia_v620'):
        days=sorted(g['day'].unique()); n_days=len(days)
        if n_days<20:
            out['oos'].append({'strategia':strategy,'dni':n_days,'status':'ZA_MALO_DNI',
                               'min_dni':20,'oos_potwierdza':False})
            continue
        cut=max(1,int(n_days*0.7)); train=g[g['day'].isin(days[:cut])]; test=g[g['day'].isin(days[cut:])]
        a=stats(train); b=stats(test)
        sufficient=len(train)>=30 and len(test)>=20 and train['day'].nunique()>=14 and test['day'].nunique()>=5
        out['oos'].append({'strategia':strategy,'dni':n_days,'train_n':len(train),'test_n':len(test),
                           'train_avg_R':a['sredni_R'],'oos_avg_R':b['sredni_R'],
                           'status':'TEST_EKSPLORACYJNY' if sufficient else 'ZA_MALO_DANYCH',
                           'oos_potwierdza':False})
    # Ranking hipotez do sprawdzenia na przyszłych, niezależnych danych.
    # NIE używać go do modyfikacji progów bramki ani live.
    for row in out['groups']:
        if row['transakcje']>=30 and row['dni']>=14:
            out['filters'].append({**row,'zastosowanie':'HIPOTEZA_TYLKO_DO_PRZYSZLEGO_TESTU'})
    out['filters'].sort(key=lambda x:x['sredni_R'],reverse=True)
    out['meta']={'status':'DIAGNOSTYKA_TYLKO','transakcje_rozstrzygniete':len(d),
                 'dni':int(d['day'].nunique()),'od':str(d['day'].min()) if len(d) else None,
                 'do':str(d['day'].max()) if len(d) else None,
                 'metoda':'chronologiczne 70/30 po dniach, bez strojenia na OOS',
                 'ostrzezenie':'Wielokrotne testowanie podgrup moze powodowac falszywe odkrycia; brak automatycznego handlu.'}
    return out

# ================= V6.21: AUDYT STRAT / DANYCH / REZIMU =================
# Wylacznie diagnostyka. Brak zmian score, do_handlu, bramki i zlecen.
V621_COLUMNS = {
    'losses': ['strategia','wynik','transakcje','dni','sredni_R','suma_R','koszt_R_sredni','status'],
    'btc': ['strategia','rezim_btc','transakcje','dni','sredni_R','suma_R','status'],
    'quality': ['symbol','side','entry_type','status_danych','powody','status_oi'],
    'rejections': ['powod','setupy','udzial_pct','uwaga'],
    'walkforward': ['strategia','dni','train_dni','test_dni','train_n','test_n','train_R','test_R','status','potwierdzony']
}

def v621_diagnose(history, live, oi_audit, rejection_rows, v620):
    """Chronologiczny audyt eksploracyjny, bez dostrajania do testu.

    Wyniki R traktujemy jako wartosci juz uwzgledniajace koszty w oryginalnym
    backtescie. Nie przypisujemy straty do SL/prowizji, jesli brak jawnej
    informacji o przyczynie lub koszcie.
    """
    from collections import Counter
    out={k:[] for k in V621_COLUMNS}
    meta={'version':'V6.21','status':'BRAK_HISTORII','min_dni_oos':20,
          'ostrzezenie':'Diagnostyka, nie dowod przewagi. Nie zmienia sygnalow handlowych.'}
    oi={r.get('symbol'):r.get('status_oi_v618','BRAK') for r in oi_audit}
    for r in live:
        sym=r.get('symbol'); problems=[]
        if not r.get('tradable',False): problems.append('RYNEK_LUB_DANE_NIETRADOWALNE')
        if r.get('price') is None or r.get('entry') is None: problems.append('BRAK_CENY')
        if r.get('spread_bps') is None: problems.append('BRAK_SPREADU')
        oi_status=oi.get(sym,'BRAK_AUDYTU_OI')
        if oi_status!='OK':problems.append('OI_'+oi_status)
        status='DANE_OK' if not problems else ('DANE_NIEWIARYGODNE' if any('NIEWIARYGODNE' in x or 'NIETRADOWALNE' in x for x in problems) else 'DANE_NIEPELNE')
        out['quality'].append({'symbol':sym,'side':r.get('side'),'entry_type':r.get('entry_type'),
                               'status_danych':status,'powody':' | '.join(problems),'status_oi':oi_status})
    counts=Counter()
    for row in rejection_rows:
        for cause in set(str(row.get('powody_v618') or '').split(' | ')):
            if cause.strip():counts[cause.strip()]+=1
    for cause,n in counts.most_common():
        out['rejections'].append({'powod':cause,'setupy':n,
            'udzial_pct':round(100*n/max(1,len(rejection_rows)),2),
            'uwaga':'Przyczyny nakladaja sie; suma udzialow moze przekroczyc 100%.'})
    if history is None or not len(history):
        out['meta']=meta;return out
    d=history.copy()
    required={'r','snapshot_utc','typ_wersja','side'}
    if not required.issubset(d.columns):
        meta['status']='BRAK_KOLUMN';meta['brakujace']=sorted(required-set(d.columns));out['meta']=meta;return out
    d['r']=pd.to_numeric(d['r'],errors='coerce')
    d['date']=pd.to_datetime(d['snapshot_utc'],utc=True,errors='coerce')
    d=d.dropna(subset=['r','date']).copy()
    if 'filled' in d.columns:d=d[d['filled'].astype(str).str.lower().isin(('true','1','1.0'))].copy()
    if d.empty:out['meta']=meta;return out
    d['day']=d['date'].dt.strftime('%Y-%m-%d')
    d['strategia']=d['typ_wersja'].astype(str)+' / '+d['side'].astype(str)
    if 'entry_type' in d.columns:d['strategia']+=' / '+d['entry_type'].astype(str)
    d['wynik']=np.where(d['r']<0,'STRATA',np.where(d['r']>0,'ZYSK','ZERO'))
    cost_col=next((c for c in ('cost_R','koszt_R','fees_R') if c in d.columns),None)
    if cost_col:d['v621_cost']=pd.to_numeric(d[cost_col],errors='coerce')
    else:d['v621_cost']=np.nan
    for (strategy,result),g in d.groupby(['strategia','wynik']):
        days=g['day'].nunique()
        out['losses'].append({'strategia':strategy,'wynik':result,'transakcje':len(g),
            'dni':int(days),'sredni_R':round(float(g['r'].mean()),4),
            'suma_R':round(float(g['r'].sum()),3),
            'koszt_R_sredni':round(float(g['v621_cost'].mean()),4) if g['v621_cost'].notna().any() else None,
            'status':'MALA_PROBA' if days<14 or len(g)<30 else 'DIAGNOSTYKA'})
    btc_col='btc_trend_1h' if 'btc_trend_1h' in d.columns else None
    if btc_col:
        btc_num=pd.to_numeric(d[btc_col],errors='coerce')
        d['btc_v621']=btc_num.map({0:'BTC_BOK',1:'BTC_WZROST',-1:'BTC_SPADEK'}).fillna('BRAK')
    else:d['btc_v621']='BRAK_DANYCH_BTC'
    for (strategy,regime),g in d.groupby(['strategia','btc_v621']):
        days=g['day'].nunique()
        out['btc'].append({'strategia':strategy,'rezim_btc':regime,'transakcje':len(g),
            'dni':int(days),'sredni_R':round(float(g['r'].mean()),4),
            'suma_R':round(float(g['r'].sum()),3),
            'status':'ZA_MALO_DANYCH' if days<14 or len(g)<30 else 'DO_WERYFIKACJI_OOS'})
    for strategy,g in d.groupby('strategia'):
        days=sorted(g['day'].unique());n=len(days)
        row={'strategia':strategy,'dni':n,'train_dni':0,'test_dni':0,
             'train_n':0,'test_n':0,'train_R':None,'test_R':None,
             'status':'ZA_MALO_DNI','potwierdzony':False}
        if n>=20:
            cut=max(1,int(n*.7));train=g[g['day'].isin(days[:cut])];test=g[g['day'].isin(days[cut:])]
            row.update(train_dni=train['day'].nunique(),test_dni=test['day'].nunique(),
                train_n=len(train),test_n=len(test),
                train_R=round(float(train['r'].mean()),4) if len(train) else None,
                test_R=round(float(test['r'].mean()),4) if len(test) else None,
                status='TEST_EKSPLORACYJNY' if len(train)>=30 and len(test)>=20 and test['day'].nunique()>=5 else 'ZA_MALO_DANYCH')
        out['walkforward'].append(row)
    meta.update(status='DIAGNOSTYKA',dni=int(d['day'].nunique()),transakcje=int(len(d)),
        dane_oi_niepelne=sum(x['status_danych']!='DANE_OK' for x in out['quality']),
        oos_potwierdzone=0,
        koszt_info='Koszt osobno dostepny' if cost_col else 'Brak oddzielnej kolumny kosztow; nie mozna przypisac strat do prowizji/poslizgu',
        przyczyna_sl_info='Bez jawnych danych o wyjsciu nie przypisujemy strat do SL')
    out['meta']=meta
    return out

# ================= V6.22: AUDYT JAKOSCI OI I STRUKTURY =================
# Diagnostyka nie zmienia do_handlu, score ani statusu bramki.
V622_OI_COLUMNS=['symbol','status_oi','wiarygodnosc_oi','rynek_tradowalny',
                 'punkty_oi','wiek_oi_min','skok_oi_pct','powody','uwaga']
V622_STRUCTURE_COLUMNS=['symbol','side','entry_type','v617_status','v617_struktura',
                        'v617_struktura_5m','v617_potwierdzenie','v622_struktura_15m_ok',
                        'v622_trend_1h_ok','v622_wybicie_5m_ok','v622_rvol_5m_ok',
                        'v622_pivot_high_15m','v622_pivot_low_15m',
                        'v622_rvol_5m','v622_brakujace_warunki','v617_powod',
                        'status_bramki','do_handlu']
V622_SUMMARY_COLUMNS=['modul','powod','liczba','odsetek_pct']

def v622_audit(live, oi_audit):
    from collections import Counter
    oi_rows=[]; structure=[]; counts=Counter()
    # OI i tradowalnosc sa niezaleznymi osiami: brak danych OI nie dowodzi
    # falszywej ceny ani nietradowalnego rynku.
    oi_map={str(r.get('symbol')):r for r in oi_audit}
    by_symbol={}
    for r in live:
        sym=str(r.get('symbol') or '')
        if sym and sym not in by_symbol:by_symbol[sym]=r
        row={k:r.get(k) for k in V622_STRUCTURE_COLUMNS}
        if r.get('v617_status')!='OK':
            row['v622_brakujace_warunki']='BRAK_DANYCH_STRUKTURY'
        structure.append(row)
        for check in ('v622_struktura_15m_ok','v622_trend_1h_ok',
                      'v622_wybicie_5m_ok','v622_rvol_5m_ok'):
            if r.get('v617_status')=='OK' and r.get(check) is False:
                counts[('STRUKTURA',check)]+=1
        if r.get('v617_status')!='OK':
            counts[('STRUKTURA','BRAK_DANYCH_STRUKTURY')]+=1
    for sym,r in oi_map.items():
        status=str(r.get('status_oi_v618') or 'BRAK')
        points=int(r.get('oi_hist_punkty_30d') or 0)
        age=r.get('oi_hist_wiek_min')
        jump=r.get('max_skok_oi_pct')
        reasons=[]
        if status=='OK': quality='POTWIERDZONE'
        elif status in ('SKOK_OI_DO_WERYFIKACJI','NIEWIARYGODNE'):
            quality='DO_WERYFIKACJI'
            reasons.append(status)
        else:
            quality='BRAK_POTWIERDZENIA'
            reasons.append(status)
        if points<2: reasons.append('MALO_POMIAROW')
        if age is not None and float(age)>360: reasons.append('STARY_POMIAR')
        market=by_symbol.get(sym,{}).get('tradable')
        oi_rows.append({'symbol':sym,'status_oi':status,'wiarygodnosc_oi':quality,
                        'rynek_tradowalny':market,'punkty_oi':points,
                        'wiek_oi_min':age,'skok_oi_pct':jump,
                        'powody':' | '.join(reasons),
                        'uwaga':'Stan OI nie jest rownoznaczny z tradowalnoscia rynku'})
        counts[('OI',quality)]+=1
    total_struct=max(1,len(structure));total_oi=max(1,len(oi_rows))
    summary=[{'modul':m,'powod':reason,'liczba':n,
              'odsetek_pct':round(n*100/(total_oi if m=='OI' else total_struct),2)}
             for (m,reason),n in sorted(counts.items())]
    return {'oi':oi_rows,'structure':structure,'summary':summary,
            'meta':{'version':'V6.22','setups':len(structure),'instrumenty_oi':len(oi_rows),
                    'uwaga':'Audyt diagnostyczny; bez zmian bramki i automatycznego handlu'}}

# ================= V6.24: MARKET/LIMIT I PRZYCZYNY OI =================
# Audyt hipotez. Nie zmienia gate, score, progow ani decyzji handlowych.
V623_ENTRY_COLUMNS=['strategia','entry_type','wariant','transakcje','dni','sredni_R','mediana_R','winrate_pct','suma_R','max_obsuniecie_R','train_R','test_R','train_n','test_n','test_dni','status','uwaga']
V623_OI_COLUMNS=['symbol','status_oi','kategoria','powody','punkty_oi','dni_oi','wiek_min','max_skok_pct','oi_do_wolumenu_24h','uwaga']
V623_FILTER_COLUMNS=['entry_type','warunek','n_setupow','spelnia','nie_spelnia','brak_danych','odsetek_spelnienia_pct','uwaga']

def v623_diagnose(history,live,oi_audit):
    from collections import Counter
    import math
    result={'entry':[],'oi':[],'filters':[], 'meta':{'version':'V6.24','status':'DIAGNOSTYKA_TYLKO','bez_zmiany_gate':True}}
    # OI: rozkladamy flagi na niezalezne, jawne powody; nie wnioskujemy z samego statusu.
    for row in oi_audit:
        status=str(row.get('status_oi_v618') or 'BRAK')
        def number(v):
            try:
                x=float(v)
                return x if math.isfinite(x) else None
            except (TypeError,ValueError):return None
        points=number(row.get('oi_hist_punkty_30d'))
        days=number(row.get('oi_hist_dni'))
        age=number(row.get('oi_hist_wiek_min'))
        jump=number(row.get('max_skok_oi_pct'))
        ratio=number(row.get('oi_do_wolumenu_24h'))
        causes=[]
        if points is None or points<2:causes.append('BRAK_LUB_MALO_PUNKTOW')
        if days is None or days<1:causes.append('BRAK_POKRYCIA_DNI')
        if age is None:causes.append('BRAK_WIEKU_POMIARU')
        elif age>360:causes.append('POMIAR_STARSZY_NIZ_6H')
        if jump is None:causes.append('BRAK_DANYCH_O_SKOKU')
        elif jump>100:causes.append('SKOK_OI_PONAD_100_PROC_DIAGNOSTYKA')
        if ratio is None:causes.append('BRAK_RELACJI_OI_WOLUMEN')
        if status!='OK':causes.append('STATUS_ZRODLOWY_'+status)
        result['oi'].append({'symbol':row.get('symbol'),'status_oi':status,
            'kategoria':'OK' if status=='OK' and not causes else 'WYMAGA_SPRAWDZENIA',
            'powody':' | '.join(causes),'punkty_oi':points,'dni_oi':days,'wiek_min':age,
            'max_skok_pct':jump,'oi_do_wolumenu_24h':ratio,
            'uwaga':'Progi 6h i 100% sa diagnostyczne, nie zmieniaja bramki'})
    for entry in ('MARKET','LIMIT'):
        rows=[r for r in live if str(r.get('entry_type','')).upper()==entry]
        for col in ('v622_struktura_15m_ok','v622_trend_1h_ok','v622_wybicie_5m_ok','v622_rvol_5m_ok'):
            yes=sum(r.get(col) is True for r in rows)
            no=sum(r.get(col) is False for r in rows)
            missing=len(rows)-yes-no
            result['filters'].append({'entry_type':entry,'warunek':col,'n_setupow':len(rows),
                'spelnia':yes,'nie_spelnia':no,'brak_danych':missing,
                'odsetek_spelnienia_pct':round(100*yes/max(1,yes+no),2),
                'uwaga':'Obserwacja live, nie wynik historycznej strategii'})
    if history is None or len(history)==0:
        result['meta']['historia']='BRAK';return result
    d=history.copy()
    required={'r','snapshot_utc','typ_wersja','side','entry_type'}
    if not required.issubset(d.columns):
        result['meta']['historia']='BRAK_KOLUMN';result['meta']['brakujace']=sorted(required-set(d.columns));return result
    d['r']=pd.to_numeric(d['r'],errors='coerce')
    d['date']=pd.to_datetime(d['snapshot_utc'],utc=True,errors='coerce')
    d=d.dropna(subset=['r','date']).copy()
    if 'filled' in d.columns:d=d[d['filled'].astype(str).str.lower().isin(('true','1','1.0'))].copy()
    if d.empty:result['meta']['historia']='BRAK_ROZSTRZYGNIETYCH';return result
    d['day']=d['date'].dt.strftime('%Y-%m-%d')
    d['strategia']=d['typ_wersja'].astype(str)+' / '+d['side'].astype(str)
    d['entry_type']=d['entry_type'].astype(str).str.upper()
    # Dane archiwalne nie zawieraja historycznych v622_*: nie przypisujemy im
    # dzisiejszych flag i nie udajemy backtestu filtrów wybicia/RVOL.
    for (strategy,entry),g in d.groupby(['strategia','entry_type']):
        g=g.sort_values('date');days=sorted(g['day'].unique());n=len(days)
        curve=g['r'].cumsum();drawdown=float((curve-curve.cummax().clip(lower=0)).min())
        cut=max(1,int(n*.7));train=g[g['day'].isin(days[:cut])];test=g[g['day'].isin(days[cut:])]
        eligible=n>=20 and len(train)>=30 and len(test)>=20 and test['day'].nunique()>=5
        result['entry'].append({'strategia':strategy,'entry_type':entry,'wariant':'BAZOWY_BEZ_HISTORYCZNYCH_FLAG_V622',
            'transakcje':len(g),'dni':n,'sredni_R':round(float(g['r'].mean()),4),
            'mediana_R':round(float(g['r'].median()),4),'winrate_pct':round(float((g['r']>0).mean()*100),2),
            'suma_R':round(float(g['r'].sum()),4),'max_obsuniecie_R':round(drawdown,4),
            'train_R':round(float(train['r'].mean()),4) if len(train) else None,
            'test_R':round(float(test['r'].mean()),4) if len(test) else None,
            'train_n':len(train),'test_n':len(test),'test_dni':test['day'].nunique(),
            'status':'TEST_EKSPLORACYJNY' if eligible else 'ZA_MALO_DANYCH',
            'uwaga':'Brak historycznych flag v622: nie mozna policzyc wplywu wybicia i RVOL na skutecznosc'})
    result['meta'].update(historia='OK',dni=int(d['day'].nunique()),transakcje=int(len(d)),
        test_filtrow='NIEWYKONANY_BRAK_HISTORYCZNYCH_FLAG',
        wniosek='Oddzielne statystyki MARKET/LIMIT to nie test skutecznosci filtrow v622')
    return result


# ================= V6.24: HISTORIA FLAG I KONTROLA JEDNOSTEK =================
# Rejestrujemy wartosci filtrów W CHWILI sygnalu. Zapis nie zmienia bramki.
V624_COLUMNS=['snapshot_utc','snapshot_ms','symbol','side','entry_type','setup_type',
              'entry','price','sl','tp1','rr_tp1_net','status_bramki','do_handlu',
              'v617_status','v617_struktura','v622_struktura_15m_ok',
              'v622_trend_1h_ok','v622_wybicie_5m_ok','v622_rvol_5m_ok',
              'v622_rvol_5m','v622_brakujace_warunki','oi_status','oi_punkty',
              'oi_wiek_min','oi_skok_pct','oi_jednostka','wolumen_jednostka',
              'porownanie_oi_wolumen','data_quality']
V624_HISTORY_NAME='v624_sygnaly_historyczne.jsonl'


# ================= V6.25: ROZLICZANIE SYGNALOW OOS (BEZ ZMIANY BRAMKI) =================
V625_HORIZON_MS=24*HOUR_MS
V625_DEDUP_MS=4*HOUR_MS
V625_MAX_HISTORY=60000
V625_ROUNDTRIP_COST_PCT=0.16  # szacunek, NIE potwierdzona prowizja WEEX

def v625_evaluate(PP,now_ms,drive_ok):
    """V6.26: rozliczaj SL/TP natychmiast po zamknieciu swiecy; diagnozuj luki."""
    from collections import defaultdict
    history_path=Path(DRIVE_DIR)/V624_HISTORY_NAME if drive_ok else OUT/V624_HISTORY_NAME
    raw=[]
    if history_path.exists():
        try:
            with history_path.open(encoding='utf-8') as fh:
                for line in fh:
                    try:
                        x=json.loads(line)
                        if isinstance(x,dict): raw.append(x)
                    except (ValueError,TypeError): continue
        except Exception as exc:
            print('V6.26: blad historii:',str(exc)[:140])
    raw=raw[-V625_MAX_HISTORY:]
    raw.sort(key=lambda x:int(number(x.get('snapshot_ms'))))
    seen=set();outcomes=[];pending=defaultdict(int);coverage=[]
    for x in raw:
        sym=str(x.get('symbol') or '')
        side=str(x.get('side') or '').upper()
        mode=str(x.get('entry_type') or '').upper()
        typ=str(x.get('setup_type') or '')
        t=int(number(x.get('snapshot_ms')))
        e=number(x.get('entry'));sl=number(x.get('sl'));tp=number(x.get('tp1'))
        if not(sym and side in ('LONG','SHORT') and mode in ('MARKET','LIMIT') and t and e>0 and sl>0 and tp>0):continue
        risk=(e-sl) if side=='LONG' else (sl-e)
        if risk<=0 or ((tp-e) if side=='LONG' else (e-tp))<=0:continue
        key=(sym,side,mode,typ,t//V625_DEDUP_MS)
        if key in seen:continue
        seen.add(key)
        a=(PP.get(sym) or {}).get('arr',{}).get('5m')
        if a is None or len(a)==0:
            pending['BRAK_SWIEC']+=1;continue
        step=INTERVAL_MS['5m'];end_t=t+V625_HORIZON_MS
        # Pierwsza swieca otwarta nie wczesniej niz chwila snapshotu.
        # W ten sposob nie odczytujemy high/low sprzed sygnalu.
        first=((t+step-1)//step)*step
        candles=[bar for bar in a if first<=float(bar[T])<end_t
                 and float(bar[T])+step<=now_ms]
        if not candles:
            pending['BRAK_PRZYSZLYCH_SWIEC']+=1;continue
        stamps=[int(float(bar[T])) for bar in candles]
        # Brak swiec posrod okresu => wynik niepewny, nie rozliczaj.
        if stamps[0]>first+step or any(b-a>step for a,b in zip(stamps,stamps[1:])):
            pending['LUKA_SWIEC']+=1
            coverage.append({'symbol':sym,'snapshot_ms':t,'first_expected':first,
                             'first_available':stamps[0],'last_available':stamps[-1],
                             'status':'LUKA_SWIEC'})
            continue
        fill=False;fill_ms=None;result=None;exit_kind='PENDING';exit_ms=None
        for bar in candles:
            hi=float(bar[H]);lo=float(bar[L]);ms=int(bar[T])
            if not fill:
                if mode=='MARKET':
                    fill=True;fill_ms=ms
                elif (side=='LONG' and lo<=e) or (side=='SHORT' and hi>=e):
                    fill=True;fill_ms=ms
                else:continue
            hit_sl=(lo<=sl if side=='LONG' else hi>=sl)
            hit_tp=(hi>=tp if side=='LONG' else lo<=tp)
            if hit_sl:
                result=-1.0;exit_kind='SL' if not hit_tp else 'SL_TP_JEDNA_SWIECA_SL_PIERWSZY'
                exit_ms=ms;break
            if hit_tp:
                result=(tp-e)/risk if side=='LONG' else (e-tp)/risk
                exit_kind='TP1';exit_ms=ms;break
        mature=now_ms>=end_t and stamps[-1]+step>=end_t-step
        if result is None and not mature:
            pending['OTWARTY_LUB_NIEWYPELNIONY']+=1;continue
        if result is None and not fill:
            exit_kind='LIMIT_NIEWYPELNIONY';net=None
        else:
            if result is None:
                last=float(candles[-1][C])
                result=(last-e)/risk if side=='LONG' else (e-last)/risk
                exit_kind='TIME_24H';exit_ms=stamps[-1]
            net=result-(e*V625_ROUNDTRIP_COST_PCT/100)/risk
        outcomes.append({'symbol':sym,'side':side,'entry_type':mode,'setup_type':typ,
            'snapshot_ms':t,'snapshot_utc':x.get('snapshot_utc'),'entry':e,'sl':sl,
            'tp1':tp,'fill_ms':fill_ms,'exit_ms':exit_ms,'status':exit_kind,
            'filled':fill,'R_net_szacunek':round(net,5) if net is not None else None,
            'koszt_model':'0.16% roundtrip; bez slippage/funding',
            'struktura_15m':x.get('v622_struktura_15m_ok'),
            'trend_1h':x.get('v622_trend_1h_ok'),
            'wybicie_5m':x.get('v622_wybicie_5m_ok'),
            'rvol_5m_ok':x.get('v622_rvol_5m_ok')})
    write_csv('v625_rozliczone_sygnaly.csv',outcomes)
    write_csv('v626_luki_historii.csv',coverage)
    groups=[]
    for field in ('side','entry_type','struktura_15m','trend_1h','wybicie_5m','rvol_5m_ok'):
        for val in sorted(set(str(x.get(field)) for x in outcomes)):
            z=[x for x in outcomes if str(x.get(field))==val]
            filled=[x for x in z if x['filled'] and x['R_net_szacunek'] is not None]
            days=len(set(str(x['snapshot_utc'])[:10] for x in filled))
            avg=sum(x['R_net_szacunek'] for x in filled)/len(filled) if filled else None
            groups.append({'wymiar':field,'wartosc':val,'sygnaly':len(z),
                'wypelnione':len(filled),'dni':days,
                'sredni_R_net_szacunek':round(avg,4) if avg is not None else None,
                'status':'ZA_MALO_DANYCH' if len(filled)<60 or days<7 else 'DO_WALIDACJI_OOS',
                'uwaga':'Grupy nakladaja sie; brak dowodu przewagi OOS'})
    write_csv('v625_wplyw_filtrow.csv',groups)

    # V6.27: porownania w ramach kierunku i rodzaju zlecenia, bez mieszania probek.
    from collections import Counter
    def yes(v):
        return str(v).strip().lower() in ('true','1','yes','tak')
    settled=[x for x in outcomes if x.get('filled') and x.get('R_net_szacunek') is not None]
    strata=[]; comparisons=[]
    filters=('struktura_15m','trend_1h','wybicie_5m','rvol_5m_ok')
    for side_name in ('LONG','SHORT'):
        for mode_name in ('MARKET','LIMIT'):
            subset=[x for x in settled if x['side']==side_name and x['entry_type']==mode_name]
            day_count=len(set(str(x.get('snapshot_utc') or '')[:10] for x in subset))
            for flag_name in filters:
                pair={}
                for state in (False,True):
                    part=[x for x in subset if yes(x.get(flag_name))==state]
                    n=len(part); vals=[float(x['R_net_szacunek']) for x in part]
                    wins=sum(v>0 for v in vals)
                    unique_symbols=len(set(x['symbol'] for x in part))
                    unique_days=len(set(str(x.get('snapshot_utc') or '')[:10] for x in part))
                    top_symbol_count=max(Counter(x['symbol'] for x in part).values(),default=0)
                    top_day_count=max(Counter(str(x.get('snapshot_utc') or '')[:10] for x in part).values(),default=0)
                    mean=sum(vals)/n if n else None
                    strata.append({'side':side_name,'entry_type':mode_name,'filtr':flag_name,
                        'filtr_spelniony':state,'liczba':n,'sredni_R_net':round(mean,5) if mean is not None else None,
                        'win_rate_pct':round(100*wins/n,2) if n else None,
                        'dni':unique_days,'symbole':unique_symbols,
                        'udzial_top_symbol_pct':round(100*top_symbol_count/n,2) if n else None,
                        'udzial_top_dzien_pct':round(100*top_day_count/n,2) if n else None,
                        'status':'DO_WALIDACJI_OOS' if n>=60 and unique_days>=7 and unique_symbols>=10 and top_symbol_count/n<=0.25 else 'ZA_MALO_LUB_SKUPIONE_DANE'})
                    pair[state]=(n,mean,unique_days,unique_symbols,top_symbol_count)
                no,yes_row=pair[False],pair[True]
                sufficient=(no[0]>=30 and yes_row[0]>=30 and
                            no[2]>=7 and yes_row[2]>=7 and
                            no[3]>=10 and yes_row[3]>=10 and
                            no[4]/max(1,no[0])<=0.25 and yes_row[4]/max(1,yes_row[0])<=0.25)
                comparisons.append({'side':side_name,'entry_type':mode_name,'filtr':flag_name,
                    'n_bez':no[0],'n_z':yes_row[0],
                    'sredni_R_bez':round(no[1],5) if no[1] is not None else None,
                    'sredni_R_z':round(yes_row[1],5) if yes_row[1] is not None else None,
                    'roznica_R_z_minus_bez':round(yes_row[1]-no[1],5) if no[1] is not None and yes_row[1] is not None else None,
                    'status':'DO_WALIDACJI_OOS' if sufficient else 'NIE_WNIOSKUJ_Z_MALEJ_PROBKI'})
    write_csv('v627_filtry_w_podgrupach.csv',strata)
    write_csv('v627_porownanie_filtrow.csv',comparisons)
    # Koncentracja po instrumentach i dniach: jeden trend moze dominowac srednia.
    concentration=[]
    for group_by in ('symbol','day'):
        keyfn=(lambda x:x['symbol']) if group_by=='symbol' else (lambda x:str(x.get('snapshot_utc') or '')[:10])
        counts=Counter(keyfn(x) for x in settled)
        for k,n in counts.most_common():
            z=[float(x['R_net_szacunek']) for x in settled if keyfn(x)==k]
            concentration.append({'wymiar':group_by,'wartosc':k,'liczba':n,
                'udzial_pct':round(100*n/max(1,len(settled)),2),
                'sredni_R_net':round(sum(z)/n,5)})
    write_csv('v627_koncentracja_wynikow.csv',concentration)

    meta={'version':'V6.27','history_rows_read':len(raw),'unique_candidates':len(seen),
          'resolved':len(outcomes),'filled':sum(bool(x['filled']) for x in outcomes),
          'closed_by_sl_tp':sum(x['status'] in ('SL','TP1','SL_TP_JEDNA_SWIECA_SL_PIERWSZY') for x in outcomes),
          'pending':dict(pending),'gap_samples':len(coverage),'horizon_hours':24,
          'model':'OHLC 5m; rozliczanie TP/SL wczesniej; konserwatywne SL w tej samej swiecy',
          'limitations':'Nie odtwarza intrabar, spreadu, poslizgu ani fundingu; snapshot MARKET to przyblizenie',
          'trade_gate_changed':False,'automatic_approval':False}
    save_json('v625_diagnostyka.json',meta,pretty=True)
    save_json('v626_diagnostyka.json',meta,pretty=True)
    save_json('v627_diagnostyka.json',meta,pretty=True)
    print('V6.27: rozliczone',len(outcomes),'SL/TP',meta['closed_by_sl_tp'],
          'oczekujace',sum(pending.values()),'luki',len(coverage),'BRAMKA BEZ ZMIAN')

def v624_capture(live,oi_audit,now_ms,drive_ok):
    from datetime import datetime,timezone
    ts=datetime.fromtimestamp(int(now_ms)/1000,timezone.utc).isoformat()
    oi_map={str(x.get('symbol')):x for x in oi_audit}
    rows=[]
    for x in live:
        sym=str(x.get('symbol') or '')
        oi=oi_map.get(sym,{})
        oi_status=str(oi.get('status_oi_v618') or 'BRAK')
        # OI_HISTORY_NAME opisuje OI jako USDT, ale jednostki kazdej
        # konkretnej wartosci wolumenu NIE sa gwarantowane.
        oi_unit='USDT (archiwum OI v63)' if oi else 'NIEZNANA'
        vol_unit='NIEPOTWIERDZONA'
        row={k:x.get(k) for k in V624_COLUMNS}
        row.update(snapshot_utc=ts,snapshot_ms=int(now_ms),symbol=sym,
                   oi_status=oi_status,oi_punkty=oi.get('oi_hist_punkty_30d'),
                   oi_wiek_min=oi.get('oi_hist_wiek_min'),
                   oi_skok_pct=oi.get('max_skok_oi_pct'),
                   oi_jednostka=oi_unit,wolumen_jednostka=vol_unit,
                   porownanie_oi_wolumen='NIE_WYKONUJ_BEZ_WERYFIKACJI_JEDNOSTEK',
                   data_quality='BRAK_FLAG' if x.get('v617_status')!='OK' else 'FLAGI_ZAPISANE')
        rows.append(row)
    pd.DataFrame(rows,columns=V624_COLUMNS).to_csv(OUT/'v624_sygnaly_snapshot.csv',index=False)
    path=Path(DRIVE_DIR)/V624_HISTORY_NAME if drive_ok else OUT/V624_HISTORY_NAME
    try:
        path.parent.mkdir(parents=True,exist_ok=True)
        with path.open('a',encoding='utf-8') as f:
            for row in rows:
                f.write(json.dumps(row,ensure_ascii=False,default=json_default)+'\n')
        persistence='DRIVE' if drive_ok else 'TYLKO_BIEZACY_ZIP'
        saved=len(rows)
    except Exception as exc:
        persistence='BLAD_ZAPISU: '+str(exc)[:160]
        saved=0
    save_json('v624_diagnostyka.json',{
        'version':'V6.24','snapshot_utc':ts,'sygnaly':len(rows),
        'zapisane':saved,'trwalosc':persistence,
        'historia_plik':str(path),
        'test_wplywu_filtrow':'NIEPOTWIERDZONY_DO_CZASU_POWSTANIA_HISTORYCZNYCH_WYNIKOW',
        'porownanie_oi_wolumen':'WSTRZYMANE_NIEPOTWIERDZONE_JEDNOSTKI',
        'bramka_bez_zmian':True},pretty=True)
    print('V6.24: zapis flag',saved,'sygnalow; archiwum:',persistence)

def main():
    t_start = time.time()
    if OUT.exists():
        shutil.rmtree(OUT)
    OUT.mkdir(parents=True)
    replay = bool(REPLAY_ZIP)
    drive_ok = mount_drive() if (USE_DRIVE and IN_COLAB and not replay) else False
    positions = load_positions(drive_ok)
    ALWAYS_INCLUDE.extend(p['symbol'] for p in positions)
    pos_rows = []
    _czasy.clear()
    _api['n'] = 0
    n_api_pobieranie = None
    if replay:
        print(f'REPLAY: {REPLAY_ZIP}')
        with etap('dane (replay)'):
            coins, now_ms = load_replay(REPLAY_ZIP)
    else:
        known = {}
        if INCREMENTAL_KLINES:
            with etap('archiwum swiec - odczyt'):
                try:
                    known = load_candle_archive(drive_ok)
                except Exception as exc:
                    print(f'Archiwum swiec: blad odczytu ({str(exc)[:120]}) - pelne pobieranie')
                    known = {}
        fund_cache = load_funding_cache(drive_ok)
        with etap('pobieranie API'):
            coins, now_ms = collect(known, fund_cache)
        n_fz = sum(1 for d in coins.values() if d.get('funding_z_pamieci'))
        n_api_pobieranie = _api['n']
        print(f"Zapytania API: {_api['n']} | funding z pamieci: {n_fz}/{len(coins)} | swiece bez pobierania: "
              f"{sum(1 for d in coins.values() for v in (d.get('klines_przyrost') or {}).values() if v == 0)}")
        save_funding_cache(coins, fund_cache, drive_ok)
    run_utc = utc_now()
    print(f'Dane: {len(coins)} kontraktow, czas serwera {iso(now_ms)}')

    PP = {sym: prepare(d, now_ms) for sym, d in coins.items()}
    if not replay:
        try:
            with etap('archiwum swiec - zapis'):
                n_arch = extend_with_archive(PP, drive_ok, now_ms)
            if n_arch:
                b5 = PP.get('BTCUSDT', {}).get('arr', {}).get('5m')
                span = (now_ms - b5[0, T]) / DAY_MS if b5 is not None and len(b5) else 0
                print(f'Archiwum swiec WEEX: {n_arch} kontraktow, historia 5m BTC: {span:.1f} dni')
        except Exception as exc:
            print(f'Archiwum swiec: blad ({str(exc)[:150]}) - dzialam na swiezych danych')
    _arch_cache.clear()
    with etap('zapis coins/*.json'):
        for sym, d in coins.items():
            P = PP[sym]
            if d.get('klines_przyrost') or P.get('_z_archiwum'):
                for tf, n in INTERVALS.items():           # pelne okno (archiwum + nowe), zeby REPLAY dzialal
                    a = P['arr'].get(tf)
                    if a is None:
                        continue
                    rows = candles_to_rows(a[-n:], tf)
                    form = P['forming'].get(tf)
                    if form is not None:
                        rows += candles_to_rows(form[None, :], tf)
                    d['klines'][tf] = {'data': rows, 'last_closed': form is None}
            save_json(f'coins/{sym}.json', d)
    btc = PP.get('BTCUSDT')
    btc_A = view(btc) if btc else None
    oi_hist = load_oi_history(drive_ok)
    us_open = us_session_open(now_ms)
    qc, summary, live = {}, [], []
    chg = {}
    for sym, P in PP.items():
        d = P['raw']
        A = view(P)
        issues = dict(d.get('errors') or {})
        luki = gaps_qc(P)
        if luki:
            issues['luki_swiec'] = luki
        if not enough(A):
            issues['za_malo_swiec'] = {tf: len(A.get(tf, [])) for tf in MIN_BARS}
            qc[sym] = issues
            continue
        tick = d.get('ticker') or {}
        price = live_price(P) or float(A['5m'][-1, C])
        form = P['forming'].get('5m')
        ref = float(form[C]) if form is not None else float(A['5m'][-1, C])
        mism = abs(price / ref - 1) * 100 if ref else 0
        if mism > 0.5:
            issues['price_mismatch_pct'] = round(mism, 3)
        f, setups = analyze(A, price, now_ms, P['fees'], btc_A if sym != 'BTCUSDT' else None,
                            crowd={'funding_z': funding_z_at(d, now_ms)})
        for pos in positions:
            if pos['symbol'] == sym and f.get('atr1'):
                try:
                    pos_rows.append(manage_position(pos, A, f, price, now_ms, P['fees'], setups))
                except Exception as exc:
                    pos_rows.append({'symbol': sym, 'side': pos['side'], 'akcja': 'BLAD', 'powody': str(exc)[:150]})
        chg[sym] = f.get('chg_24h_pct')
        book = book_stats(d.get('depth'), price)
        fund = funding_stats(d)
        oi = oi_usdt(d, price)
        past = [x for x in oi_hist.get(sym, []) if now_ms - x[0] > 60000]
        oich = oi_changes(past, now_ms, oi) if oi else {}
        if oi:
            oi_hist[sym] = past + [[now_ms, oi]]
        qv = number(tick.get('quoteVolume'))
        oi_ok = not (oi and qv and oi > OI_MAX_DO_WOLUMENU * qv)
        if not oi_ok:
            issues['oi_niewiarygodne'] = f'OI {oi / 1e6:.0f}M USDT = {oi / qv:.0f}x wolumenu 24h'
        reasons = []
        last6 = A['5m'][-6:]
        if (P['asset_type'] in STOCK_TYPES and not us_open) or (
                P['asset_type'] in TRADFI_TYPES and ((last6[:, V] <= 0).all() or (last6[:, H] <= last6[:, L]).all())):
            reasons.append('TradFi zamkniety')
        if book['spread_bps'] is None or book['spread_bps'] > MAX_SPREAD_BPS or (book['depth_usdt_0.5pct'] or 0) < MIN_DEPTH_USDT or qv < MIN_QUOTE_VOL_USDT:
            reasons.append('book/wolumen')
        min_oi = MIN_OI_TRADFI_USDT if P['asset_type'] in TRADFI_TYPES else MIN_OI_USDT
        if oi_ok and (oi or 0) < min_oi:
            reasons.append(f'OI < {min_oi / 1e6:.1f}M')
        if f['vol_anomaly']:
            reasons.append('anomalia wolumenu')
        if mism > PRICE_MISMATCH_PCT:
            reasons.append('cena niespojna')
        wiek = (now_ms - (A['5m'][-1, T] + INTERVAL_MS['5m'])) / 60000   # V6.10: wiek ostatniej zamknietej swiecy 5m
        bledy = [k for k in (d.get('errors') or {}) if k.startswith('klines_')]
        if wiek > DANE_MAX_WIEK_MIN or 'klines_5m' in bledy:
            issues['dane_nieaktualne'] = {'wiek_5m_min': round(float(wiek), 1), 'bledy': bledy}
            reasons.append('dane nieaktualne')
        tradable = not reasons
        if issues:
            qc[sym] = issues
        row = {'symbol': sym, 'live_price': price, 'tradable': tradable, 'not_tradable_reason': ', '.join(reasons),
               'asset_type': P['asset_type'], 'quote_vol_24h': qv, 'oi_usdt': rnd(oi, 0) if oi_ok else None,
               'oi_wiarygodne': oi_ok, 'oi_surowe_usdt': None if oi_ok else rnd(oi, 0), **{k: rnd(v, 3) for k, v in oich.items()},
               **{k: rnd(v, 6 if k == 'funding_rate' else 2) for k, v in fund.items()},
               **{k: rnd(v, 2) for k, v in book.items()},
               'taker_fee_bps': rnd(P['fees'][0], 2), 'maker_fee_bps': rnd(P['fees'][1], 2)}
        for k in FEATURE_COLS + ['struct_1h', 'last_event_1h', 'struct_15m', 'last_event_15m', 'vol_anomaly']:
            v = f.get(k)
            row[k] = rnd(v, 3) if isinstance(v, (float, np.floating)) else v
        row['setups'] = len(setups)
        summary.append(row)
        for r in setups:
            try:
                r.update(v616_market_context(A, f, r.get('side')))
                r.update(v616_target_diagnostics(r, A, f))
                r.update(v617_regime(A, f, r.get('side'), now_ms))
                r.update(v617_levels(r, A, f, now_ms))
            except Exception as exc:
                r['v616_blad_diagnostyki'] = str(exc)[:120]
            if P['asset_type'] in STOCK_TYPES:
                r['filtr'] = ';'.join(x for x in (r['filtr'], 'tradfi_bez_testu') if x)
            r.update(symbol=sym, price=price, snapshot_utc=snap_iso(now_ms), tradable=tradable, asset_type=P['asset_type'],
                     slip_bps=rnd(slip_for(P), 2),
                     not_tradable_reason=row['not_tradable_reason'],
                     dist_entry_pct=rnd((r['entry'] / price - 1) * 100, 3),
                     wazne_do_utc=None if r['entry_type'] == 'MARKET' else iso(now_ms + r['wait_h'] * HOUR_MS)[:16],
                     wazne_do_pl=None if r['entry_type'] == 'MARKET' else czas_pl(now_ms + r['wait_h'] * HOUR_MS),
                     cancel_if=(f"wejscie teraz; nieaktualne, gdy cena wyjdzie ze strefy wejscia (poziom {r['zone_ref']})"
                                if r['entry_type'] == 'MARKET' else
                                f"anuluj o {czas_pl(now_ms + r['wait_h'] * HOUR_MS)} czasu PL ({r['wait_h']}h) bez wypelnienia "
                                f"albo gdy cena dojdzie do {r['tp1']} przed wejsciem"),
                     funding_z=row.get('funding_z'), oi_chg_4h_pct=row.get('oi_chg_4h_pct'),
                     spread_bps=row.get('spread_bps'))
            try:
                r.update(leverage_plan(r, P))
            except Exception as exc:
                r['dzwignia_uwagi'] = f'blad planu dzwigni: {str(exc)[:80]}'
            live.append(r)
    ranks = rank_pct(chg)
    vrank = {x['symbol']: i + 1 for i, x in enumerate(sorted(summary, key=lambda x: -(x.get('quote_vol_24h') or 0)))}
    for r in summary:
        r['rank_24h'] = ranks.get(r['symbol'])
        r['vol_rank'] = vrank.get(r['symbol'])
    for r in live:
        r['rank_24h'] = ranks.get(r['symbol'])
        cechy_ruchu(r)
        r['vol_rank'] = vrank.get(r['symbol'])
    if not replay:
        save_oi_history(oi_hist, drive_ok, now_ms)
    gidx, gate_mkt = {}, None                        # V6.13: tlum z Gate.io (inna gielda) - tylko informacja
    try:
        coins = sorted([x for x in summary if x.get('asset_type') == 'COIN'], key=lambda x: -(x.get('quote_vol_24h') or 0))
        gsyms = [x['symbol'] for x in coins[:GATE_TOP_N]]
        coin_set = {x['symbol'] for x in coins}
        for r in sorted(live, key=lambda r: vrank.get(r['symbol'], 999)):
            if len(gsyms) >= GATE_MAX_SYM:
                break
            if r['symbol'] in coin_set and r['symbol'] not in gsyms:
                gsyms.append(r['symbol'])
        gidx = gate_index(load_gate_stats(drive_ok, now_ms, gsyms, replay))
        for r in live:
            if r['symbol'] in gidx:
                r.update(gate_features(gidx[r['symbol']], now_ms))
                gate_side_flags(r)
        for r in summary:
            if r['symbol'] in gidx:
                r.update(gate_features(gidx[r['symbol']], now_ms))
        gate_mkt = gate_market(gidx, now_ms) if gidx else None
    except Exception as exc:
        print(f'Gate.io: blad ({str(exc)[:150]}) - pomijam')
    try:
        log_cols = ['symbol', 'live_price', 'funding_rate', 'funding_z', 'basis_bps', 'oi_usdt', 'oi_chg_1h_pct', 'oi_chg_4h_pct',
                    'taker_buy_1h_pct', 'chg_1h_pct', 'chg_24h_pct', 'book_imbalance_0.5pct']
        log = pd.DataFrame([{k: r.get(k) for k in log_cols} for r in summary])
        log.insert(0, 'snapshot_utc', snap_iso(now_ms))
        log.to_csv(OUT / 'crowd_snapshot.csv', index=False)
        if drive_ok and not replay:
            fp = Path(DRIVE_DIR) / CROWD_LOG
            log.to_csv(fp, mode='a', header=not fp.exists(), index=False)
    except Exception as exc:
        print(f'Log fundingu/OI: blad ({str(exc)[:120]})')

    bt = []
    if BACKTEST:
        print('Test wsteczny ...')
        with etap('test wsteczny 7 dni'):
            bt = run_backtest(PP, now_ms, oi_arr=oi_arrays(oi_hist), gidx=gidx)
    bt_df = write_csv('backtest.csv', bt, BT_FIRST)
    arch = update_archive(bt, run_utc, drive_ok, now_ms)
    arch.to_csv(OUT / 'backtest_archive.csv', index=False)
    arch_tf, arch = split_stocks(arch)               # V6.14: akcje/indeksy osobno - logika krypto bez zmian
    tf_tab = tradfi_table(arch_tf, now_ms)
    if len(tf_tab):
        tf_tab.to_csv(OUT / 'tradfi_test.csv', index=False)
    gate = gate_table(arch, now_ms)
    gate.to_csv(OUT / 'gate.csv', index=False)
    gmap = {(g.typ, g.side): g for g in gate.itertuples()}
    for r in live:
        g = gmap.get((r['typ_wersja'], r['side']))
        r['status_bramki'] = g.status if g is not None else 'OBSERWACJA'
        r['typ_transakcje'], r['typ_sredni_R'], r['typ_t'] = (g.transakcje, g.sredni_R, g.t) if g is not None else (0, None, None)
    hist_df, hist_meta = pd.DataFrame(), {}
    if HIST_BACKTEST and not replay:
        try:
            with etap('test historyczny 30 dni'):
                hist_df, hist_meta = run_hist(PP, now_ms, drive_ok)
        except Exception as exc:
            print(f'Test historyczny nieudany: {str(exc)[:200]}')
    if replay and not len(hist_df):
        try:                                           # replay: test historyczny z zipa, zeby ocena miala historie
            with zipfile.ZipFile(REPLAY_ZIP) as z:
                nm = next((n for n in z.namelist() if n.endswith('hist_backtest.csv')), None)
                if nm:
                    hist_df = pd.read_csv(io.BytesIO(z.read(nm)))
        except Exception as exc:
            print(f'Replay: brak testu historycznego ({str(exc)[:100]})')
    htab = hist_table(hist_df)
    var_df = hist_meta.pop('_warianty', None) if isinstance(hist_meta, dict) else None
    if var_df is not None and len(var_df):
        var_df.to_csv(OUT / 'hist_warianty.csv', index=False)
    if len(hist_df):
        hist_df.to_csv(OUT / 'hist_backtest.csv', index=False)
        htab.to_csv(OUT / 'hist_summary.csv', index=False)
    cechy = cechy_table(hist_df, f'historia {HIST_DAYS}d') + cechy_table(arch, f'archiwum WEEX {BT_ARCHIVE_DAYS}d')
    if cechy:
        pd.DataFrame(cechy).to_csv(OUT / 'hist_cechy.csv', index=False)
    order = {'DO GRY': 0, 'OBSERWACJA': 1}
    score_setups(live, arch, now_ms, hist_df)
    apply_tradfi(live, tf_tab)
    btc_bias = btc_short_bias(btc_A)
    rate_setups(live, btc_bias)
    ochrona = protection_mode(arch, now_ms)
    for r in live:
        if ochrona_strony(ochrona, r['side']):
            try:
                r.update(leverage_plan(r, PP[r['symbol']], OCHRONA_MNOZNIK))
                r['dzwignia_uwagi'] = '; '.join(x for x in (r.get('dzwignia_uwagi'), f'TRYB OCHRONNY: ryzyko x{OCHRONA_MNOZNIK}') if x)
            except Exception:
                pass
    shock = btc_shock(btc, live_price(btc)) if btc else None
    dzien = journal_day(drive_ok, now_ms)
    try:
        squeeze = squeeze_watch(PP, summary, now_ms, drive_ok)
    except Exception as exc:
        squeeze = None
        print(f'Squeeze watch: blad ({str(exc)[:150]})')
    warunki_glob = apply_conditions(live, shock, ochrona, dzien)
    study = None
    if BADANIE_RYNKU:
        try:
            with etap('badanie rynku'):
                study = market_study(PP)
            if study:
                apply_study(live, study)
                pd.DataFrame(study['kierunek']).to_csv(OUT / 'kierunek.csv', index=False)
                pd.DataFrame(study['przegrzanie']).to_csv(OUT / 'przegrzanie.csv', index=False)
        except Exception as exc:
            print(f'Badanie rynku: blad ({str(exc)[:150]})')
    for r in live:
        r['w_limicie_ryzyka'] = False                  # V6.9: ustawiane na True w wybor_ryzyka
    rank_rows = ranking(live)
    ekspozycja = {sd: {'setupy_AB': len({r['symbol'] for r in rank_rows if r['side'] == sd and r['ocena'] in ('A', 'B')}),
                       'pozycje': sum(1 for p in pos_rows if p.get('side') == sd)} for sd in ('LONG', 'SHORT')}
    for sd, e in ekspozycja.items():                   # V6.5: MARKET i LIMIT na tym samym kontrakcie = jedna transakcja
        ryz = RYZYKO_NA_TRANSAKCJE_PCT * (OCHRONA_MNOZNIK if ochrona_strony(ochrona, sd) else 1)
        e['ryzyko_pct'] = rnd((e['setupy_AB'] + e['pozycje']) * ryz, 2)
        e['za_duze'] = e['ryzyko_pct'] > MAX_RYZYKO_KIERUNEK_PCT
        e['wybrane'] = wybor_ryzyka(rank_rows, sd, ryz, e['pozycje'])
    # V6.16: osobne diagnostyki; brak automatycznego odblokowywania transakcji.
    v616_oos = v616_oos_report(arch)
    write_csv('v616_oos.csv', v616_oos)
    v617_wf = v617_walkforward(arch)
    write_csv('v617_walkforward.csv', v617_wf)
    v618_oi = v618_oi_audit(summary, oi_hist, now_ms)
    v618_reject = v618_rejection_audit(live)
    v618_coverage = v618_history_coverage(arch)
    write_csv('v618_oi_audit.csv', v618_oi)
    write_csv('v618_odrzucenia.csv', v618_reject)
    write_csv('v618_historia.csv', v618_coverage)
    save_json('v618_diagnostyka.json', {'oi':v618_oi,'odrzucenia':v618_reject,'historia':v618_coverage}, pretty=True)
    # V6.20: analiza warunkowej skutecznosci, bez ingerencji w score/gate.
    v620 = v620_diagnose(arch if len(arch) else hist_df)
    write_csv('v620_wyniki_warunkowe.csv', v620['groups'])
    write_csv('v620_walkforward.csv', v620['oos'])
    # V6.21: stabilny schemat CSV, nawet gdy nie ma zadnej hipotezy.
    v621_filter_cols = ['strategia','wymiar','wartosc','transakcje','dni','sredni_R','mediana_R','winrate_pct','suma_R','status','zastosowanie']
    pd.DataFrame(v620['filters'], columns=v621_filter_cols).to_csv(OUT / 'v620_hipotezy_filtrow.csv', index=False)
    v621 = v621_diagnose(arch if len(arch) else hist_df, live, v618_oi, v618_reject, v620)
    for name, key in [('v621_straty.csv','losses'), ('v621_rezim_btc.csv','btc'),
                      ('v621_jakosc_danych.csv','quality'), ('v621_powody_odrzucen.csv','rejections'),
                      ('v621_walkforward.csv','walkforward')]:
        pd.DataFrame(v621[key], columns=V621_COLUMNS[key]).to_csv(OUT / name, index=False)
    save_json('v621_diagnostyka.json', v621['meta'], pretty=True)
    print('V6.21: diagnostyka strat i BTC:', v621['meta']['status'], '(bez zmiany bramki)')
    save_json('v620_diagnostyka.json', v620, pretty=True)
    print('V6.20: diagnostyka historyczna:', v620['meta'].get('status'),
          'dni:', v620['meta'].get('dni'), '(BEZ nowych zatwierdzen)')
    # V6.22: niezalezny audyt danych OI i skladowych potwierdzenia struktury.
    v622 = v622_audit(live, v618_oi)
    for name,key,columns in [
        ('v622_oi_diagnostyka.csv','oi',V622_OI_COLUMNS),
        ('v622_struktura_diagnostyka.csv','structure',V622_STRUCTURE_COLUMNS),
        ('v622_przyczyny.csv','summary',V622_SUMMARY_COLUMNS)]:
        pd.DataFrame(v622[key],columns=columns).to_csv(OUT/name,index=False)
    save_json('v622_diagnostyka.json',v622['meta'],pretty=True)
    print('V6.22: audyt OI i struktury:',len(v622['oi']),len(v622['structure']),
          '(bez zmiany bramki)')
    v623=v623_diagnose(arch if len(arch) else hist_df,live,v618_oi)
    for name,key,columns in [('v623_market_limit.csv','entry',V623_ENTRY_COLUMNS),
                             ('v623_oi_przyczyny.csv','oi',V623_OI_COLUMNS),
                             ('v623_filtry_live.csv','filters',V623_FILTER_COLUMNS)]:
        pd.DataFrame(v623[key],columns=columns).to_csv(OUT/name,index=False)
    save_json('v623_diagnostyka.json',v623['meta'],pretty=True)
    print('V6.24: MARKET/LIMIT i OI:',v623['meta'].get('historia'),'bez zmiany bramki')
    v624_capture(live,v618_oi,now_ms,drive_ok)
    v625_evaluate(PP,now_ms,drive_ok)
    v619_drive = v619_drive_status(drive_ok, arch, oi_hist, now_ms)
    v619_oi = v619_oi_quality(v618_oi)
    v619_reasons = v619_rejection_summary(v618_reject)
    v619_observe = v619_watchlist(live)
    write_csv('v619_drive_status.csv',v619_drive)
    write_csv('v619_oi_quality.csv',v619_oi)
    write_csv('v619_powody_odrzucen.csv',v619_reasons)
    write_csv('v619_watchlist.csv',v619_observe)
    v619_drive_saved = v619_save_drive_diagnostics(drive_ok,now_ms,{
        'version':VERSION,'run_utc':iso(now_ms),'drive':v619_drive,
        'oi':v619_oi,'rejections':v619_reasons,'watchlist':v619_observe})
    save_json('v619_diagnostyka.json',{'drive':v619_drive,'oi':v619_oi,
        'odrzucenia':v619_reasons,'watchlist':v619_observe,
        'drive_zapis':v619_drive_saved},pretty=True)
    print(f'V6.19: Drive={drive_ok}; zapis diagnostyki: {v619_drive_saved}; watchlist={len(v619_observe)} (TYLKO OBSERWACJA)')
    write_csv('v617_kontekst_setups.csv', [{k:v for k,v in r.items() if k in
        ('symbol','side','entry_type','entry','sl','tp1','status_bramki','do_handlu',
         'v617_status','v617_struktura','v617_struktura_5m','v617_korekta_5m',
         'v617_potwierdzenie','v617_rvol','v617_powod','v617_poziomy_status',
         'v617_sl_sug','v617_tp1_sug','v617_rr_brutto','v617_rr_ok','v617_liczba_testow')}
         for r in live])
    write_csv('v616_kontekst_setups.csv', [{k:v for k,v in r.items() if k in
        ('symbol','side','entry_type','entry','sl','tp1','status_bramki','do_handlu',
         'v616_rezim','v616_struktura_5m','v616_struktura_15m','v616_rvol_5m',
         'v616_bos_5m','v616_potwierdzenie','v616_cel_status','v616_sl_atr',
         'v616_tp1_struktura','v616_rr_brutto','v616_rr_min_1_5')}
        for r in live])
    write_csv('ranking.csv', rank_rows, RANK_FIRST)
    write_csv('ranking_market.csv', [r for r in rank_rows if r['entry_type'] == 'MARKET'], RANK_FIRST)
    write_csv('ranking_limit.csv', [r for r in rank_rows if r['entry_type'] == 'LIMIT'], RANK_FIRST)
    live.sort(key=lambda r: (order.get(r['status_bramki'], 2), r['filtr'] != '', not r['tradable'], -r['score_R'],
                             r['dist_entry_atr']))
    for pos in positions:
        if not any(r['symbol'] == pos['symbol'] and r['side'] == pos['side'] for r in pos_rows):
            pos_rows.append({'symbol': pos['symbol'], 'side': pos['side'], 'akcja': 'BRAK DANYCH',
                             'powody': 'kontrakt nie pobrany albo za malo swiec'})
    if positions:
        write_csv('pozycje.csv', pos_rows, POS_FIRST)
    write_csv('setups_all.csv', live, SETUP_FIRST)
    good = [r for r in live if not r['filtr'] and r['tradable']]
    write_csv('setups.csv', good, SETUP_FIRST)
    write_csv('setups_market.csv', [r for r in good if r['entry_type'] == 'MARKET'], SETUP_FIRST)
    write_csv('setups_limit.csv', [r for r in good if r['entry_type'] == 'LIMIT'], SETUP_FIRST)
    top = top_setups(good)
    write_csv('top_setups.csv', top, TOP_FIRST)
    summary.sort(key=lambda r: -(r['quote_vol_24h'] or 0))
    write_csv('summary.csv', summary)
    save_json('summary.json', summary)
    save_json('qc.json', qc, pretty=True)

    bf = market_features(btc_A, float(btc['arr']['5m'][-1, C]), now_ms)[0] if btc_A and enough(btc_A) else None
    meta = {'version': VERSION, 'finished_utc': utc_now(), 'server_time': iso(now_ms), 'seconds': round(time.time() - t_start, 1),
            'replay': REPLAY_ZIP or None, 'czasy_s': dict(_czasy), 'zapytania_api': n_api_pobieranie, 'symbols': len(coins), 'setups_all': len(live), 'setups_ok': len(good),
            'top': [{k: r.get(k) for k in TOP_FIRST} for r in top],
            'tryb_ochronny': ochrona, 'warunki_globalne': warunki_glob, 'dzien': dzien, 'squeeze': squeeze, 'gate_tlum': gate_mkt,
            'badanie_rynku': {k: v for k, v in (study or {}).items() if k != 'flagi'}, 'alarm_btc': shock, 'ekspozycja': ekspozycja,
            'pozycje': pos_rows, 'btc_bias_5m': btc_bias, 'us_session_open': bool(us_open),
            'ranking': [{k: r.get(k) for k in RANK_FIRST} for r in rank_rows],
            'v616_oos': v616_oos, 'v616_diagnostyka_tylko': True,
            'v617_walkforward': v617_wf, 'v617_diagnostyka_tylko': True,
            'v620_diagnostyka_tylko': True, 'v620_historia_dni': v620['meta'].get('dni'),
            'v619_diagnostyka_tylko': True, 'v619_drive_zapis':v619_drive_saved, 'v618_diagnostyka_tylko': True, 'v618_oi_statusy': dict(pd.Series([x['status_oi_v618'] for x in v618_oi]).value_counts()),
            'paging_5m': _paging['supported'], 'drive': drive_ok,
            'btc_context': {k: rnd(bf.get(k), 3) if isinstance(bf.get(k), float) else bf.get(k)
                            for k in ('trend_1h', 'trend_4h', 'chg_24h_pct', 'regime_1h', 'ext_ema50_atr_1h')} if bf else None,
            'backtest_ten_przebieg': bt_summary(split_stocks(bt_df)[1]) if len(bt_df) else {},
            'backtest_archiwum': bt_summary(arch) if len(arch) else {},
            'tradfi_archiwum': bt_summary(arch_tf) if len(arch_tf) else {},
            'tradfi_test': tf_tab.to_dict('records') if len(tf_tab) else [],
            'bramka': gate.to_dict('records'),
            'test_historyczny': {'meta': {k: v for k, v in hist_meta.items() if k != 'sig'},
                                 'wyniki': htab.to_dict('records') if len(htab) else []},
            'ustawienia': {k: globals()[k] for k in ('MAX_COST_R', 'EXT_CHASE_MAX_ATR', 'SHORT_SQUEEZE_EXT_ATR', 'SETUP_MIN_RR',
                                                    'SETUP_TYPES', 'SETUP_WAIT_H', 'GATE_FROM_UTC', 'GATE_MIN_TRADES',
                                                    'GATE_MIN_AVG_R', 'GATE_MIN_T', 'BT_LOOKBACK_H', 'BT_STEP_H', 'BT_MAX_HOLD_H',
                                                    'KAPITAL_USDT', 'RYZYKO_NA_TRANSAKCJE_PCT', 'PREFEROWANA_DZWIGNIA', 'LEV_LIQ_SL_MULT',
                                                    'LEV_MMR', 'MAX_DEPOZYT_PCT')}}
    save_json('run_meta.json', meta, pretty=True)

    meta['test_historyczny']['liq_vs_tp1'] = liq_compare(hist_df[hist_df.filtr.fillna('') == '']) if len(hist_df) else {}
    save_json('run_meta.json', meta, pretty=True)
    if var_df is not None and len(var_df):
        print(f"\nWARIANTY USTAWIEN (test historyczny {HIST_DAYS} dni, setupy po filtrach; zmiana warta rozwazenia tylko, gdy lepsza w OBU polowach):")
        for v in var_df[var_df.grupa == 'WSZYSTKIE'].itertuples():
            extra = '' if v.wariant == 'BAZA' else (f" | roznica sumy R {v.roznica_suma_R:+} (1. pol. {v.roznica_p1_suma_R:+}, 2. pol. {v.roznica_p2_suma_R:+})"
                                                   f"{' -> LEPSZY W OBU POLOWACH' if v.lepszy_w_obu_polowach else ''}")
            print(f"  {v.wariant:22s} n={v.transakcje} wypeln. {v.wypelnienie_pct}% SL {v.SL_pct}% sr {v.sredni_R}R suma {v.suma_R}R ({v.R_na_dzien}R/dzien){extra}")
    if cechy:
        print(f"\nCECHY RUCHU 24h (setupy po filtrach; sredni R z cecha vs bez, 1. | 2. polowa; ocena ich nie uzywa: OCENA_CECHY_RUCHU={OCENA_CECHY_RUCHU}):")
        for c in cechy:
            print(f"  {c['zrodlo']:22s} {c['cecha']:13s} n {c['p1_n']}/{c['p2_n']}  R {c['p1_R']} vs {c['p1_R_reszta']} | "
                  f"{c['p2_R']} vs {c['p2_R_reszta']}  przewaga {c['p1_edge']} vs {c['p1_edge_reszta']} | {c['p2_edge']} vs {c['p2_edge_reszta']} -> {c['wniosek']}")
    if study:
        print(f"\nBADANIE RYNKU ({study['dni_historii']} dni swiec 1h, {study['kontrakty']} kontraktow; regula dziala tylko przy t >= {BADANIE_MIN_T} w OBU polowach):")
        print('  KIERUNEK (koszyk altow, nastepne 12h, gra w strone sygnalu):')
        for k in study['kierunek']:
            print(f"    {k['sygnal']:16s} teraz {k['teraz']:+d}  1. pol. {k['p1_bps']}bps t {k['p1_t']} | 2. pol. {k['p2_bps']}bps t {k['p2_t']} -> {k['wniosek']}")
        print('  PRZEGRZANIE (setup przeciw ruchowi: SL za ekstremum swiecy +0.3 ATR, TP 2R, max 24h; wynik w R po kosztach):')
        for p in study['przegrzanie']:
            print(f"    {p['sygnal']:16s} {p['ruch']:7s} -> {p['gra']:15s} n={p['zdarzenia']:5d} 1. pol. {p['p1_R']}R t {p['p1_t']} | 2. pol. {p['p2_R']}R t {p['p2_t']} -> {p['wniosek']}"
                  f"  (bez SL, 24h: {p['bez_SL_24h_p1_bps']} / {p['bez_SL_24h_p2_bps']} bps)")
    print(f'\nV6.19: walk-forward {len(v617_wf)} grup; poziomy i struktura w v617_kontekst_setups.csv (diagnostyka).')
    print(f'\nV6.16: raport OOS: {len(v616_oos)} grup; kontekst i dynamiczne poziomy w v616_kontekst_setups.csv (tylko diagnostyka).')
    print_report(good, live, split_stocks(bt_df)[1] if len(bt_df) else bt_df, arch, gate, {**meta, '_hist_ok': hist_df[hist_df.filtr.fillna('') == ''] if len(hist_df) else None}, top, htab)
    stamp = datetime.fromtimestamp(now_ms / 1000, tz=timezone.utc).strftime('%Y%m%d_%H%M%S')
    zpath = OUT_ROOT / f'weex_v6_{stamp}.zip'
    with etap('zip'):
        with zipfile.ZipFile(zpath, 'w', zipfile.ZIP_DEFLATED, compresslevel=1) as z:   # V6.8: szybka kompresja
            for p in OUT.rglob('*'):
                if p.is_file():
                    z.write(p, p.relative_to(OUT_ROOT))
    print(f'\nZapisano {zpath} ({zpath.stat().st_size / 1e6:.1f} MB) w {time.time() - t_start:.0f}s')
    print('Czasy etapow: ' + ', '.join(f'{k} {v:.0f}s' for k, v in _czasy.items()))
    if IN_COLAB:
        files.download(str(zpath))
    return zpath


# ============================== BADANIE RYNKU: KIERUNEK I PRZEGRZANIE ==============================
def _half_stats(pnl, step):
    """pnl: Series (indeks czasu) wynikow w strone sygnalu; t na nienakladajacych sie probkach co `step`."""
    out = []
    pnl = pnl.dropna()
    if not len(pnl):
        return [(None, None, 0)] * 3
    mid = pnl.index[0] + (pnl.index[-1] - pnl.index[0]) / 2
    for part in (pnl[pnl.index < mid], pnl[pnl.index >= mid], pnl):
        sub = part.iloc[::step] if step > 1 else part
        t = float(sub.mean() / sub.std() * math.sqrt(len(sub))) if len(sub) > 2 and sub.std() > 0 else None
        out.append((float(part.mean() * 1e4) if len(part) else None, t, int(len(sub))))
    return out


def _verdict(st):
    (b1, t1, _), (b2, t2, _), _ = st
    if None in (b1, b2, t1, t2):
        return 'za malo danych'
    if b1 >= BADANIE_MIN_BPS and b2 >= BADANIE_MIN_BPS and t1 >= BADANIE_MIN_T and t2 >= BADANIE_MIN_T:
        return 'dziala'
    if b1 <= -BADANIE_MIN_BPS and b2 <= -BADANIE_MIN_BPS and t1 <= -BADANIE_MIN_T and t2 <= -BADANIE_MIN_T:
        return 'odwrotnie'
    return 'brak przewagi'


def _verdict_R(st, min_r=0.05):
    """Jak _verdict, ale dla wyniku w R (setup z SL): obie polowy >= min_r R i t >= BADANIE_MIN_T (albo odwrotnie)."""
    (b1, t1, _), (b2, t2, _), _ = st
    if None in (b1, b2, t1, t2):
        return 'za malo danych'
    if b1 >= min_r and b2 >= min_r and t1 >= BADANIE_MIN_T and t2 >= BADANIE_MIN_T:
        return 'dziala'
    if b1 <= -min_r and b2 <= -min_r and t1 <= -BADANIE_MIN_T and t2 <= -BADANIE_MIN_T:
        return 'odwrotnie'
    return 'brak przewagi'


def market_study(PP):
    """Test sygnalow kierunku (koszyk altow, nastepne 12h) i przegrzania (gra przeciw ruchowi, nastepne 24h) na swiecach 1h
    kontraktow COIN. Wynik w kazdej polowie historii osobno; reguly wlaczaja sie tylko, gdy obie polowy zgodne i istotne."""
    cols = {}
    for sym, P in PP.items():
        a = P['arr'].get('1h')
        if P['asset_type'] != 'COIN' or a is None or len(a) < 600:
            continue
        cols[sym] = pd.DataFrame(a[:, [O, H, L, C, V]], index=pd.to_datetime(a[:, T], unit='ms'), columns=['o', 'h', 'l', 'c', 'v'])
    if len(cols) < 20 or 'BTCUSDT' not in cols:
        return None
    Cc = pd.DataFrame({k: v.c for k, v in cols.items()}).sort_index()
    Cc = Cc.dropna(axis=1, thresh=int(len(Cc) * 0.9)).ffill()
    alts = [x for x in Cc.columns if x != 'BTCUSDT']
    ema = lambda x, n: x.ewm(span=n, adjust=False).mean()

    def trend(c):
        e20, e50 = ema(c, 20), ema(c, 50)
        return pd.Series(np.where((c > e20) & (e20 > e50), 1, np.where((c < e20) & (e20 < e50), -1, 0)), index=c.index)
    idx = np.log(Cc[alts]).diff().mean(axis=1).cumsum()
    b = Cc['BTCUSDT']
    b4 = b.resample('4h', label='right', closed='right').last().dropna()
    sig = {'btc_trend_1h': trend(b),
           'btc_trend_4h': trend(b4).reindex(Cc.index, method='ffill'),
           'szerokosc_ema50': (Cc[alts] > Cc[alts].apply(lambda x: ema(x, 50))).mean(axis=1) * 2 - 1,
           'szerokosc_trend': pd.DataFrame({x: trend(Cc[x]) for x in alts}).mean(axis=1),
           'momentum_24h': idx.diff(24), 'momentum_72h': idx.diff(72)}
    fwd = (idx.shift(-12) - idx)
    kier = []
    for name, v in sig.items():
        v = v.iloc[80:]
        sgn = np.sign(v)
        pnl = (sgn * fwd.reindex(v.index))[sgn != 0]
        st = _half_stats(pnl, 12)
        now = float(sgn.iloc[-1]) if len(sgn) else 0
        kier.append({'sygnal': name, 'wniosek': _verdict(st), 'teraz': int(now),
                     'p1_bps': rnd(st[0][0], 1), 'p1_t': rnd(st[0][1], 2), 'p2_bps': rnd(st[1][0], 1), 'p2_t': rnd(st[1][1], 2),
                     'calosc_bps': rnd(st[2][0], 1), 'calosc_t': rnd(st[2][1], 2), 'okna': st[2][2],
                     'od': str(v.index[0])[:16], 'do': str(v.index[-1])[:16]})
    # przegrzanie: zdarzenia per kontrakt, gra PRZECIW ruchowi od otwarcia nastepnej swiecy, wynik po 24h minus koszt
    ev = {}
    now_flags = {}
    for sym, df in cols.items():
        if sym not in Cc.columns:
            continue
        c, h, l = df.c, df.h, df.l
        tr = pd.concat([h - l, (h - c.shift()).abs(), (l - c.shift()).abs()], axis=1).max(axis=1)
        a1 = tr.ewm(alpha=1 / 14, adjust=False).mean()
        ext = (c - ema(c, 50)) / a1
        d = c.diff()
        up = d.clip(lower=0).ewm(alpha=1 / 14, adjust=False).mean()
        dn = (-d.clip(upper=0)).ewm(alpha=1 / 14, adjust=False).mean()
        r = 100 - 100 / (1 + up / dn.replace(0, np.nan))
        ch = (c / c.shift(24) - 1) * 100
        defs = {'RSI': (r > PRZEGRZANIE_RSI, r < 100 - PRZEGRZANIE_RSI),
                'wychylenie_EMA50': (ext > PRZEGRZANIE_EXT_ATR, ext < -PRZEGRZANIE_EXT_ATR),
                'ruch_24h': (ch > PRZEGRZANIE_24H_PCT, ch < -PRZEGRZANIE_24H_PCT)}
        nxt = df.o.shift(-1)
        out24 = c.shift(-24)
        hv, lv, cv, ov, av = h.values, l.values, c.values, df.o.values, a1.values
        flags = {}
        start = df.index[min(60, len(df) - 1)]                      # bez rozgrzewki EMA/RSI
        for name, (mu, md) in defs.items():
            flags[name] = 1 if bool(mu.iloc[-1]) else -1 if bool(md.iloc[-1]) else 0
            for move, m in ((1, mu), (-1, md)):
                m = m.fillna(False)
                m = m & ~m.shift(1).rolling(6, min_periods=1).max().fillna(0).astype(bool)
                ok = m.values & out24.notna().values & nxt.notna().values & (df.index >= start)
                res24 = (-move * np.log(out24 / nxt) - BADANIE_KOSZT)[ok]
                # "ryzykowny setup przeciw ruchowi": wejscie na otwarciu nastepnej swiecy, SL za ekstremum swiecy
                # sygnalu + 0.3 ATR, TP 2R, max 24h; SL przed TP w tej samej swiecy (pesymistycznie). Wynik w R po kosztach.
                rr = []
                for i in np.where(ok)[0]:
                    sd = -move
                    e = ov[i + 1]
                    sl = (hv[i] + 0.3 * av[i]) if sd == -1 else (lv[i] - 0.3 * av[i])
                    risk = abs(e - sl)
                    if risk <= 0 or risk / e > 0.1 or sd * (e - sl) <= 0:
                        rr.append(np.nan); continue
                    tp = e + sd * 2 * risk
                    out = None
                    for j in range(i + 1, min(i + 25, len(cv))):
                        if (hv[j] >= sl) if sd == -1 else (lv[j] <= sl):
                            out = -1.0; break
                        if (lv[j] <= tp) if sd == -1 else (hv[j] >= tp):
                            out = 2.0; break
                    if out is None:
                        out = sd * (cv[min(i + 24, len(cv) - 1)] - e) / risk
                    rr.append(out - BADANIE_KOSZT * e / risk)
                ev.setdefault((name, move), []).append(pd.DataFrame({'bps': res24.values, 'R': rr}, index=res24.index))
        now_flags[sym] = flags
    prz = []
    for (name, move), parts in ev.items():
        parts = [p for p in parts if len(p)]
        if not parts:
            continue
        x = pd.concat(parts)
        per = x.groupby(x.index.floor('D')).mean().sort_index()       # dni = klastry (alty ruszaja sie razem)
        st = _half_stats(per['bps'], 1)
        stR = _half_stats(per['R'].dropna() / 1e4, 1)                  # _half_stats mnozy przez 1e4 -> wynik w R
        v = _verdict_R(stR)
        prz.append({'sygnal': name, 'ruch': 'wzrost' if move == 1 else 'spadek',
                    'gra': 'SHORT (przeciw)' if move == 1 else 'LONG (przeciw)',
                    'wniosek': {'dziala': 'odwrot dziala', 'odwrotnie': 'ruch trwa dalej'}.get(v, v),
                    'zdarzenia': int(len(x)), 'dni': stR[2][2],
                    'p1_R': rnd(stR[0][0], 3), 'p1_t': rnd(stR[0][1], 2), 'p2_R': rnd(stR[1][0], 3), 'p2_t': rnd(stR[1][1], 2),
                    'calosc_R': rnd(stR[2][0], 3), 'calosc_t': rnd(stR[2][1], 2),
                    'bez_SL_24h_p1_bps': rnd(st[0][0], 1), 'bez_SL_24h_p2_bps': rnd(st[1][0], 1)})
    return {'kierunek': kier, 'przegrzanie': prz, 'flagi': now_flags,
            'dni_historii': rnd((Cc.index[-1] - Cc.index[0]).total_seconds() / 86400, 1), 'kontrakty': len(Cc.columns)}


def apply_study(rows, study):
    """Warunki z badania rynku (tylko reguly, ktore przeszly test w obu polowach)."""
    if not study:
        return
    act = [k for k in study['kierunek'] if k['wniosek'] in ('dziala', 'odwrotnie') and k['teraz']]
    prz = {(p['sygnal'], 1 if p['ruch'] == 'wzrost' else -1): p['wniosek'] for p in study['przegrzanie']}
    for r in rows:
        s = 1 if r['side'] == 'LONG' else -1
        why = []
        if KIER_FILTR:
            for k in act:
                d = k['teraz'] * (1 if k['wniosek'] == 'dziala' else -1)
                if d == -s:
                    why.append(f"kierunek przeciw ({k['sygnal']})")
        fl = (study['flagi'] or {}).get(r['symbol'], {})
        hot = [n for n, v in fl.items() if v == -s]          # przegrzany ruch przeciw kierunkowi setupu
        r['przegrzanie'] = ', '.join(f"{n} {'w gore' if v == 1 else 'w dol'}" for n, v in fl.items() if v) or None
        if PRZEGRZANIE_FILTR:
            for n in hot:
                if prz.get((n, -s)) == 'ruch trwa dalej':
                    why.append(f"przegrzanie {n} {'w gore' if s == -1 else 'w dol'}: ruch zwykle trwa dalej")
        if why:
            r['do_handlu'] = False
            r['warunki'] = '; '.join(x for x in ([r.get('warunki')] if r.get('warunki') not in (None, 'OK') else []) + why)


def score_setups(rows, arch, now_ms, hist=None):
    """Ocena setupu = srednia PRZEWAGA (edge_R: wynik - losowe wejscie w tym samym kierunku) tego samego typu,
    rodzaju wejscia i kierunku, sciagnieta do zera przy malej probie: edge * n / (n + RANK_SHRINK_K).
    Zrodlo: test historyczny (HIST_DAYS dni), a gdy go brak - archiwum WEEX z RANK_DAYS dni."""
    def table(d, src):
        tab = {}
        if d is None or not len(d) or 'edge_R' not in d.columns:
            return tab
        d = d[(d.filtr.fillna('') == '') & d.status.isin(FINAL)].copy()
        d['edge_R'] = pd.to_numeric(d['edge_R'], errors='coerce')
        d = d[d.edge_R.notna()]
        for (tv, side), g in d.groupby(['typ_wersja', 'side']):
            per = g.groupby('snapshot_utc').edge_R.mean()
            t = float(per.mean() / per.std() * math.sqrt(len(per))) if len(per) > 2 and per.std() > 0 else None
            tab[(tv, side)] = (len(g), float(g.r.mean()), float(g.edge_R.mean()), src, t)
        return tab
    since = snap_iso(now_ms - RANK_DAYS * DAY_MS)
    t_arch = table(arch[arch.snapshot_utc >= since] if len(arch) else arch, f'archiwum {RANK_DAYS}d')
    t_hist = table(hist, f'historia {HIST_DAYS}d')
    for r in rows:
        key = (r['typ_wersja'], r['side'])
        n, m, e, src, t = t_hist.get(key) or t_arch.get(key) or (0, 0.0, 0.0, '-', None)
        r['hist_n'], r['hist_sr_R'], r['hist_edge_R'], r['hist_zrodlo'] = n, (rnd(m, 3) if n else None), (rnd(e, 3) if n else None), src
        r['hist_t_edge'] = rnd(t, 2)
        r['score_R'] = rnd(e * n / (n + RANK_SHRINK_K), 3) if n else 0.0


def btc_short_bias(btc_A):
    """Krotki kierunek BTC: struktura 5m z pivotami k=2 (jak CHoCH w REV_MSS), gdy brak - trend 1h. +1 / -1 / 0."""
    if not btc_A or '5m' not in btc_A or len(btc_A['5m']) < 50:
        return 0
    a5 = btc_A['5m'][-300:]
    b = structure(a5[:, H], a5[:, L], a5[:, C], 2)['bias']
    return int(b) if b else int(trend_of(btc_A['1h'][:, C]) or 0)


def momentum_zgodnosc(r, s):
    """Ile z 3 jest po stronie transakcji: cena vs VWAP dzienny, RSI 15m vs 50, zmiana 4h. None gdy brak danych."""
    v, q, c4 = r.get('vwap_d_dist_atr'), r.get('rsi_15m'), r.get('chg_4h_pct')
    if v is None or q is None or c4 is None or any(isinstance(x, float) and math.isnan(x) for x in (v, q, c4)):
        return None
    return int(s * v > 0) + int(s * (q - 50) > 0) + int(s * c4 > 0)


def rate_setups(rows, btc_bias):
    """Ocena A-D z punktow STRUKTURALNYCH (sprawdzone na 30 dniach, w tym na ostatnich 15 dniach poza dopasowaniem):
    R:R >= 2.5 (+1), koszt > MAX_COST_R (-1), przeciw trendowi 4h (-1), rezim 1h trend (+1) / range (-1),
    przeciw trendowi 1h BTC (-1), mala para (-1). Historia typu tylko jako informacja (OCENA_WAGA_HISTORII).
    Wylaczony typ / squeeze / nie do handlu -> D. Skrajny funding -> najwyzej C."""
    for r in rows:
        pkt, why = 0, []
        t = r.get('hist_t_edge') or 0
        sr = r.get('hist_sr_R')
        h = (2 if t >= 2 else 1 if t >= 1 else 0) + ((1 if sr > 0.1 else -1 if sr < 0 else 0) if sr is not None else 0)
        if OCENA_WAGA_HISTORII:
            pkt += OCENA_WAGA_HISTORII * h
        why.append(f"historia ({r.get('hist_zrodlo')}): t={t}, sr {sr:+.2f}R" if sr is not None else f'historia: brak')
        if (r.get('rr_tp1_net') or 0) >= 2.5:
            pkt += 1; why.append(f"R:R {r['rr_tp1_net']} (+1)")
        if (r.get('cost_R') or 0) > MAX_COST_R:
            pkt -= 1; why.append(f"koszt {r['cost_R']}R (-1)")
        f = r.get('filtr') or ''
        if 'przeciw_trendowi' in f:
            pkt -= 1; why.append('przeciw trendowi (-1)')
        reg = r.get('regime_1h')
        if reg == 'trend':
            pkt += 1; why.append('rezim 1h trend (+1)')
        elif reg == 'range':
            pkt -= 1; why.append('rezim 1h range (-1)')
        s = 1 if r['side'] == 'LONG' else -1
        bt = r.get('btc_trend_1h')
        corr = r.get('corr_btc')
        if bt and bt == -s and (r['symbol'] == 'BTCUSDT' or corr is None or corr >= BTC_CORR_MIN):
            pkt -= 1; why.append('przeciw trendowi 1h BTC (-1)')
        if (r.get('chop_1h') or 0) >= CHOP_MAX:
            pkt -= 1; why.append(f"CHOP 1h {r['chop_1h']} (-1)")
        if r.get('entry_type') == 'LIMIT' and (r.get('dist_entry_atr') or 0) > OCENA_MAX_DIST_ATR:
            pkt -= 1; why.append(f"wejscie daleko: {r['dist_entry_atr']} ATR (-1)")
        if OCENA_MOMENTUM:
            zg = momentum_zgodnosc(r, s)
            r['zgodnosc_momentum'] = zg
            if zg is not None and zg <= 1:
                pkt -= 1; why.append(f'momentum przeciw ({zg}/3: VWAP d, RSI 15m, 4h) (-1)')
        if OCENA_CECHY_RUCHU:
            if r.get('kontra_24h'):
                pkt += 1; why.append('przeciw ruchowi 24h (+1)')
            if r.get('pogon_24h'):
                pkt -= 1; why.append('pogon za ruchem 24h (-1)')
            if r.get('wolumen_skok'):
                pkt += 1; why.append('skok wolumenu (+1)')
        if (r.get('vol_rank') or 0) > MALE_PARY_RANK:
            pkt -= 1; why.append(f"mala para (#{r['vol_rank']} wg wolumenu) (-1)")
        rsb = r.get('rs_btc_24h')
        if OCENA_SHORT_SLABY_RS is not None and r.get('side') == 'SHORT' and rsb is not None and rsb < OCENA_SHORT_SLABY_RS:
            pkt -= 1; why.append(f"short na slabym coinie (RS vs BTC {rsb:+.1f}%) (-1)")
        grade = next((g for g, lo in OCENA_PROGI.items() if pkt >= lo), 'D')
        fz = r.get('funding_z')
        if fz is not None and abs(fz) > OCENA_FUNDING_Z_MAX and grade in ('A', 'B'):
            grade = 'C'; why.append(f'skrajny funding (z {fz}) -> max C')
        if 'typ_wylaczony' in f:
            grade = 'D'; why.append('typ wylaczony (tracil w historii)')
        if 'squeeze' in f:
            grade = 'D'; why.append('squeeze')
        if not r.get('tradable') or 'tradfi' in f or 'anomalia' in f:
            grade = 'D'; why.append('nie do handlu / bez testu')
        r['ocena'], r['ocena_pkt'], r['ocena_powody'] = grade, pkt, '; '.join(why)


def cechy_ruchu(r):
    """V6.7: cechy ruchu 24h dla setupu (wymaga rank_24h i vol_ratio_1h)."""
    rk, vr = r.get('rank_24h'), r.get('vol_ratio_1h')
    s = 1 if r.get('side') == 'LONG' else -1
    ok = rk is not None and not (isinstance(rk, float) and math.isnan(rk))
    kier = (rk if s == 1 else 100 - rk) if ok else None
    r['ruch24_kier'] = rnd(kier, 1) if ok else None
    r['kontra_24h'] = bool(ok and kier <= CECHY_KONTRA_PCT)
    r['pogon_24h'] = bool(ok and kier >= 100 - CECHY_KONTRA_PCT)
    r['wolumen_skok'] = bool(vr is not None and not (isinstance(vr, float) and math.isnan(vr)) and vr >= CECHY_VOL_SKOK)


def cechy_table(df, zrodlo):
    """V6.7: wynik setupow (po filtrach) z cecha vs bez niej, w obu polowach okresu. Cecha 'lepsza w obu polowach',
    gdy sredni R i srednia przewaga nad losowym wejsciem sa wyzsze niz bez cechy w kazdej polowie."""
    if df is None or not len(df) or 'rank_24h' not in df.columns or 'edge_R' not in df.columns:
        return []
    d = df[(df.filtr.fillna('') == '') & df.status.isin(FINAL)].copy()
    if not len(d):
        return []
    d['r'] = pd.to_numeric(d['r'], errors='coerce')
    d['edge_R'] = pd.to_numeric(d.get('edge_R'), errors='coerce')
    rk = pd.to_numeric(d['rank_24h'], errors='coerce')
    vr = pd.to_numeric(d.get('vol_ratio_1h'), errors='coerce')
    kier = np.where(d.side == 'LONG', rk, 100 - rk)
    d['kontra_24h'] = kier <= CECHY_KONTRA_PCT
    d['pogon_24h'] = kier >= 100 - CECHY_KONTRA_PCT
    d['wolumen_skok'] = vr >= CECHY_VOL_SKOK
    d = d[rk.notna()]
    snaps = sorted(d.snapshot_utc.unique())
    if len(snaps) < 4:
        return []
    mid = snaps[len(snaps) // 2]
    d['pol'] = np.where(d.snapshot_utc < mid, 1, 2)
    out = []
    extra = []                                       # V6.13: cechy OI/tlumu - tylko tam, gdzie sa dane
    if 'oi_chg_4h_bt' in d.columns:
        oi4 = pd.to_numeric(d['oi_chg_4h_bt'], errors='coerce')
        d['oi_rosnie_4h'], d['oi_spada_4h'] = oi4 >= 2, oi4 <= -2
        d['_m_oi_rosnie_4h'] = d['_m_oi_spada_4h'] = oi4.notna()
        extra += ['oi_rosnie_4h', 'oi_spada_4h']
    for c_ in ('tlum_ta_sama_strona', 'likw_tej_strony'):
        if c_ in d.columns:
            m_ = d[c_].notna()
            d['_m_' + c_] = m_
            d[c_] = d[c_].astype(str).str.lower().isin(('true', '1', '1.0'))
            extra.append(c_)
    for cecha in ('kontra_24h', 'pogon_24h', 'wolumen_skok') + tuple(extra):
        row = {'zrodlo': zrodlo, 'cecha': cecha}
        lepsze = []
        for pol in (1, 2):
            x = d[d.pol == pol]
            if '_m_' + cecha in d.columns:
                x = x[x['_m_' + cecha]]
            a, b = x[x[cecha]], x[~x[cecha]]
            row.update({f'p{pol}_n': len(a), f'p{pol}_R': rnd(a.r.mean(), 3) if len(a) else None,
                        f'p{pol}_R_reszta': rnd(b.r.mean(), 3) if len(b) else None,
                        f'p{pol}_edge': rnd(a.edge_R.mean(), 3) if len(a) else None,
                        f'p{pol}_edge_reszta': rnd(b.edge_R.mean(), 3) if len(b) else None})
            lepsze.append(len(a) >= 10 and len(b) >= 10 and a.r.mean() > b.r.mean() and a.edge_R.mean() > b.edge_R.mean())
        gorsze = all(row[f'p{p}_R'] is not None and row[f'p{p}_R_reszta'] is not None and row[f'p{p}_R'] < row[f'p{p}_R_reszta']
                     and row[f'p{p}_n'] >= 10 for p in (1, 2))
        row['wniosek'] = 'lepsze w obu polowach' if all(lepsze) else 'gorsze w obu polowach' if gorsze else 'brak zgodnosci'
        out.append(row)
    return out


def wybor_ryzyka(rank_rows, side, ryz, n_poz):
    """V6.5: najlepsze setupy A/B w kierunku, ktore mieszcza sie w MAX_RYZYKO_KIERUNEK_PCT razem z otwartymi
    pozycjami (jeden na kontrakt; najpierw 'do handlu', potem ocena, punkty, R:R). Ustawia r['w_limicie_ryzyka']."""
    miejsca = int((MAX_RYZYKO_KIERUNEK_PCT + 1e-9) // ryz) - n_poz if ryz > 0 else 0
    cand = [r for r in rank_rows if r['side'] == side and r['ocena'] in ('A', 'B')
            and 'tryb ochronny' not in (r.get('warunki') or '')]   # V6.6: strona w trybie ochronnym = nie do handlu
    cand.sort(key=lambda r: (not r.get('do_handlu'), 'ABCD'.index(r['ocena']), -r['ocena_pkt'],
                             r['entry_type'] != 'LIMIT', -(r['rr_tp1_net'] or 0)))
    wyb, out = set(), []
    for r in rank_rows:
        if r['side'] == side:
            r['w_limicie_ryzyka'] = False
    for r in cand:
        if r['symbol'] in wyb:
            continue
        if len(wyb) >= max(0, miejsca):
            break
        wyb.add(r['symbol'])
        r['w_limicie_ryzyka'] = True
        out.append(f"{r['symbol']} {r['entry_type']}")
    return out


def ranking(live):
    """Najlepsze wg oceny osobno MARKET i LIMIT, jeden setup na kontrakt w kazdym rodzaju."""
    out = []
    for mode in ('MARKET', 'LIMIT'):
        cand = [r for r in live if r['entry_type'] == mode]
        cand.sort(key=lambda r: ('ABCD'.index(r['ocena']), -r['ocena_pkt'], -(r['rr_tp1_net'] or 0), r['cost_R'] or 9))
        seen = set()
        for r in cand:
            if r['symbol'] in seen:
                continue
            seen.add(r['symbol'])
            out.append(r)
    return out


def top_setups(good):
    """Najlepsze setupy osobno dla MARKET i LIMIT. Warunki: przewaga typu+kierunku+wejscia nad losowym wejsciem
    dodatnia i t >= TOP_MIN_T; musi tez miec do_handlu=True po wszystkich filtrach. Najwyzej TOP_MAX_PER_SIDE na kierunek i jeden setup na kontrakt.
    Kolejnosc: status bramki, ocena (score_R), R:R netto, nizszy koszt."""
    order = {'DO GRY': 0, 'OBSERWACJA': 1}
    cand = [r for r in good if r.get('status_bramki') == 'DO GRY' and r.get('do_handlu') is True
            and r['score_R'] > 0 and (r.get('hist_t_edge') or 0) >= TOP_MIN_T and r.get('ocena') != 'D'
            and 'tryb ochronny' not in (r.get('warunki') or '')]   # V6.5: strona w trybie ochronnym nie trafia do TOP
    cand.sort(key=lambda r: (order.get(r['status_bramki'], 2), -r['score_R'], -(r['rr_tp1_net'] or 0), r['cost_R'] or 9))
    per_mode, per_side, used, out = {}, {}, set(), []
    for r in cand:
        mode, side = r['entry_type'], r['side']
        if r['symbol'] in used or per_mode.get(mode, 0) >= TOP_N_PER_MODE or per_side.get(side, 0) >= TOP_MAX_PER_SIDE:
            continue
        used.add(r['symbol'])
        per_mode[mode] = per_mode.get(mode, 0) + 1
        per_side[side] = per_side.get(side, 0) + 1
        t = dict(r)
        t['mode'], t['rank'] = mode, per_mode[mode]
        t['dlaczego'] = (f"{r['setup_type']} {side} {mode} ({r['hist_zrodlo']}): {r['hist_n']} tr., sr. {r['hist_sr_R']:+.2f}R, "
                         f"przewaga nad losowym wejsciem {r['hist_edge_R']:+.2f}R (t {r['hist_t_edge']}) -> ocena {r['score_R']:+.3f}R; "
                         f"R:R {r['rr_tp1_net']}, koszt {r['cost_R']}R, bramka: {r['status_bramki']}")
        out.append(t)
    out.sort(key=lambda t: (t['mode'] != 'MARKET', t['rank']))
    return out


def btc_shock(btc_P, price_now):
    """Gwaltowny ruch BTC: zmiana z ostatnich ALARM_BTC_MIN minut (swiece 5m + cena teraz) w ATR 15m."""
    if not btc_P or '5m' not in btc_P['arr'] or '15m' not in btc_P['arr']:
        return None
    a5, a15 = btc_P['arr']['5m'], btc_P['arr']['15m']
    n = max(1, ALARM_BTC_MIN // 5)
    if len(a5) < n + 1 or len(a15) < 20:
        return None
    a = atr(a15[:, H], a15[:, L], a15[:, C])
    seg = a5[-n:]
    hi, lo = max(float(seg[:, H].max()), price_now), min(float(seg[:, L].min()), price_now)
    ref = float(a5[-n - 1, C])
    move = (price_now - ref) / a if a else 0
    rng = (hi - lo) / a if a else 0
    return {'zmiana_atr15': rnd(move, 2), 'zakres_atr15': rnd(rng, 2), 'alarm': abs(move) >= ALARM_BTC_ATR15 or rng >= ALARM_BTC_ATR15 * 1.3,
            'od': rnd(ref, 1), 'teraz': rnd(price_now, 1)}


# ============================== V6.13: OI W TESCIE + TLUM Z GATE.IO (tylko informacja) ==============================
GATE_URL = 'https://api.gateio.ws/api/v4/futures/usdt/contract_stats'


def oi_arrays(oi_hist):
    """{sym: (czasy_ms, oi_usdt)} posortowane - do szybkiego wyszukiwania w tescie wstecznym."""
    out = {}
    for sym, v in (oi_hist or {}).items():
        pts = sorted((int(t), float(x)) for t, x in v if t and x)
        if pts:
            out[sym] = (np.array([p[0] for p in pts], dtype=np.int64), np.array([p[1] for p in pts]))
    return out


def _nearest(ta, va, t, tol):
    if ta is None or not len(ta):
        return None
    i = int(np.searchsorted(ta, t))
    best = None
    for j in (i - 1, i):
        if 0 <= j < len(ta) and abs(int(ta[j]) - t) <= tol and (best is None or abs(int(ta[j]) - t) < abs(int(ta[best]) - t)):
            best = j
    return float(va[best]) if best is not None else None


def oi_changes_at(arr, ts):
    """V6.13: zmiana OI 1h/4h/24h w chwili zdjecia testu (te same tolerancje co oi_changes na zywo)."""
    out = {'oi_chg_1h_bt': None, 'oi_chg_4h_bt': None, 'oi_chg_24h_bt': None}
    if not arr:
        return out
    ta, va = arr
    cur = _nearest(ta, va, ts, 30 * 60000)            # uruchomienia nie sa rowno o pelnej godzinie
    if not cur:
        return out
    for name, hrs in (('oi_chg_1h_bt', 1), ('oi_chg_4h_bt', 4), ('oi_chg_24h_bt', 24)):
        old = _nearest(ta, va, ts - hrs * HOUR_MS, max(0.35 * hrs * HOUR_MS, 20 * 60000))
        if old and 0.2 < cur / old < 5:
            out[name] = rnd((cur / old - 1) * 100, 2)
    return out


def gate_contract(sym):
    s_ = sym[:-4] if sym.endswith('USDT') else sym
    names = [f'{s_}_USDT']
    if s_.startswith('1000') and len(s_) > 4:
        names.append(f'{s_[4:]}_USDT')                       # WEEX 1000PEPE -> Gate PEPE (proporcje L/S i likwidacje USD bez zmian)
    return names


def _gate_one(sym):
    for c in gate_contract(sym):
        for lim in (100, 48):
            try:
                r = requests.get(GATE_URL, params={'contract': c, 'interval': '1h', 'limit': lim}, timeout=8)
            except Exception:
                return None
            if r.status_code == 200:
                try:
                    data = r.json()
                except Exception:
                    data = None
                if isinstance(data, list) and data:
                    return c, data
                break
            if r.status_code not in (400, 422):
                break
    return None


def gate_parse(sym, contract, data):
    rows = []
    for x in data:
        if not isinstance(x, dict) or x.get('time') is None:
            continue
        g = lambda k: (float(x[k]) if x.get(k) not in (None, '') else None)
        try:
            rows.append({'symbol': sym, 'gate_contract': contract, 'time_ms': (lambda t: t if t > 1e12 else t * 1000)(int(float(x['time']))),
                         'lsr_account': g('lsr_account'), 'lsr_taker': g('lsr_taker'),
                         'top_lsr_account': g('top_lsr_account'), 'top_lsr_size': g('top_lsr_size'),
                         'long_liq_usd': g('long_liq_usd'), 'short_liq_usd': g('short_liq_usd'),
                         'open_interest_usd': g('open_interest_usd'), 'mark_price': g('mark_price')})
        except Exception:
            continue
    return rows


def fetch_gate_stats(symbols):
    """V6.13: publiczne statystyki tlumu z Gate.io. Zwraca DataFrame (moze byc pusty). Bez klucza API."""
    rows, ok = [], 0
    with ThreadPoolExecutor(max_workers=8) as ex:
        futs = {ex.submit(_gate_one, s_): s_ for s_ in symbols}
        for f in as_completed(futs):
            try:
                res = f.result()
            except Exception:
                res = None
            if res:
                ok += 1
                rows += gate_parse(futs[f], *res)
    print(f'Gate.io (long/short, likwidacje - inna gielda): {ok}/{len(symbols)} kontraktow')
    return pd.DataFrame(rows)


def gate_index(gdf):
    """{sym: DataFrame posortowany po czasie}."""
    if gdf is None or not len(gdf):
        return {}
    return {s_: d.sort_values('time_ms').drop_duplicates('time_ms').reset_index(drop=True) for s_, d in gdf.groupby('symbol')}


def gate_features(d, ts):
    """Cechy tlumu z zamknietych godzin Gate przed chwila ts (swieca 1h liczona od time_ms do time_ms+1h)."""
    if d is None or not len(d):
        return {}
    x = d[d.time_ms + HOUR_MS <= ts]
    if not len(x) or ts - int(x.time_ms.iloc[-1]) > 3 * HOUR_MS:
        return {}
    last = x.iloc[-1]
    out = {'gate_lsr_konta': rnd(last.lsr_account, 3), 'gate_lsr_taker': rnd(last.lsr_taker, 3),
           'gate_top_lsr_konta': rnd(last.top_lsr_account, 3)}
    w = pd.to_numeric(x.lsr_account.iloc[-48:], errors='coerce').dropna()
    if len(w) >= 24 and w.std() > 0:
        out['gate_lsr_z'] = rnd((w.iloc[-1] - w.mean()) / w.std(), 2)
    l4 = x.iloc[-4:]
    ll, sl_ = pd.to_numeric(l4.long_liq_usd, errors='coerce').sum(), pd.to_numeric(l4.short_liq_usd, errors='coerce').sum()
    out['gate_likw_long_4h_usd'], out['gate_likw_short_4h_usd'] = rnd(ll, 0), rnd(sl_, 0)
    out['gate_likw_long_udzial'] = rnd(ll / (ll + sl_), 2) if ll + sl_ > 0 else None
    oi = pd.to_numeric(x.open_interest_usd, errors='coerce')
    if len(oi) >= 5 and oi.iloc[-5] and oi.iloc[-1] and 0.2 < oi.iloc[-1] / oi.iloc[-5] < 5:
        out['gate_oi_chg_4h_pct'] = rnd((oi.iloc[-1] / oi.iloc[-5] - 1) * 100, 2)
    return out


def gate_side_flags(r):
    """Cechy wzgledem strony transakcji (do hist_cechy): tlum po tej samej stronie, likwidacje tej strony przewazaja."""
    z, u = r.get('gate_lsr_z'), r.get('gate_likw_long_udzial')
    lg = r.get('side') == 'LONG'
    if z is not None:
        r['tlum_ta_sama_strona'] = bool(z >= 1 if lg else z <= -1)
    if u is not None:
        r['likw_tej_strony'] = bool(u >= 0.65 if lg else u <= 0.35)


def load_gate_stats(drive_ok, now_ms, symbols, replay):
    """Na zywo: pobiera z Gate i zapisuje; replay: czyta gate_stats.csv z zipa."""
    if not GATE_STATS:
        return pd.DataFrame()
    if replay:
        try:
            with zipfile.ZipFile(REPLAY_ZIP) as z:
                nm = next((n for n in z.namelist() if n.endswith('gate_stats.csv')), None)
                gdf = pd.read_csv(io.BytesIO(z.read(nm))) if nm else pd.DataFrame()
            if len(gdf):
                gdf.to_csv(OUT / 'gate_stats.csv', index=False)
            return gdf
        except Exception:
            return pd.DataFrame()
    try:
        gdf = fetch_gate_stats(symbols)
    except Exception as exc:
        print(f'Gate.io: blad ({str(exc)[:120]}) - pomijam')
        return pd.DataFrame()
    if len(gdf):
        gdf.to_csv(OUT / 'gate_stats.csv', index=False)
        if drive_ok:
            try:
                last = gdf.sort_values('time_ms').groupby('symbol').tail(1).copy()
                last.insert(0, 'snapshot_utc', snap_iso(now_ms))
                fp = Path(DRIVE_DIR) / GATE_LOG
                last.to_csv(fp, mode='a', header=not fp.exists(), index=False)
            except Exception as exc:
                print(f'Gate log: blad ({str(exc)[:100]})')
    return gdf


def gate_market(gidx, now_ms):
    """Podsumowanie rynku do raportu: BTC/ETH + suma likwidacji 4h."""
    out = {'kontrakty': len(gidx)}
    tl = ts_ = 0.0
    for s_, d in gidx.items():
        f = gate_features(d, now_ms)
        if s_ in ('BTCUSDT', 'ETHUSDT'):
            out[s_] = f
        tl += f.get('gate_likw_long_4h_usd') or 0
        ts_ += f.get('gate_likw_short_4h_usd') or 0
    out['likw_long_4h_usd'], out['likw_short_4h_usd'] = rnd(tl, 0), rnd(ts_, 0)
    out['likw_long_udzial'] = rnd(tl / (tl + ts_), 2) if tl + ts_ > 0 else None
    return out


# ============================== V6.14: TRADFI ==============================
def split_stocks(arch):
    """(akcje/indeksy/Pre-IPO, reszta). Wiersze bez asset_type (starsze wersje) = krypto."""
    if arch is None or not len(arch) or 'asset_type' not in arch.columns:
        return (arch.iloc[0:0] if arch is not None else pd.DataFrame()), arch
    m = arch.asset_type.isin(STOCK_TYPES)
    return arch[m].reset_index(drop=True), arch[~m].reset_index(drop=True)


def tradfi_table(d, now_ms):
    """Wyniki testu akcji/indeksow (po filtrach, bez 'tradfi_bez_testu') wg typu, kierunku i wejscia."""
    cols = ['typ_wersja', 'side', 'transakcje', 'sredni_R', 'sredni_edge_R', 't_edge', 'skutecznosc_pct', 'zdjecia', 'odblokowany']
    if d is None or not len(d) or 'edge_R' not in d.columns:
        return pd.DataFrame(columns=cols)
    d = d.copy()
    f = d.filtr.fillna('').str.replace('tradfi_bez_testu', '', regex=False).str.strip(';')
    d = d[(f == '') & d.status.isin(FINAL)]
    d['r'] = pd.to_numeric(d['r'], errors='coerce')
    d['edge_R'] = pd.to_numeric(d['edge_R'], errors='coerce')
    rows = []
    for (tv, side), g in d.groupby(['typ_wersja', 'side']):
        st = stats(g)
        ok = (st.get('transakcje', 0) >= TRADFI_MIN_N and (st.get('sredni_R') or -9) > 0
              and (st.get('sredni_edge_R') or -9) > 0 and (st.get('t_edge') or -9) >= TRADFI_MIN_T)
        rows.append({'typ_wersja': tv, 'side': side, 'transakcje': st.get('transakcje', 0), 'sredni_R': st.get('sredni_R'),
                     'sredni_edge_R': st.get('sredni_edge_R'), 't_edge': st.get('t_edge'),
                     'skutecznosc_pct': st.get('skutecznosc_pct'), 'zdjecia': st.get('zdjecia'), 'odblokowany': bool(ok)})
    return pd.DataFrame(rows, columns=cols).sort_values(['odblokowany', 'transakcje'], ascending=False).reset_index(drop=True) if rows else pd.DataFrame(columns=cols)


def apply_tradfi(live, tf_tab):
    """Setupy na akcjach: historia z testu akcji (nie z krypto); filtr 'tradfi_bez_testu' zdjety, gdy typ odblokowany."""
    tab = {(x['typ_wersja'], x['side']): x for x in (tf_tab.to_dict('records') if tf_tab is not None and len(tf_tab) else [])}
    for r in live:
        if r.get('asset_type') not in STOCK_TYPES:
            continue
        x = tab.get((r['typ_wersja'], r['side']))
        n = int(x['transakcje']) if x else 0
        e = float(x['sredni_edge_R']) if x and x.get('sredni_edge_R') is not None else 0.0
        r['hist_n'], r['hist_sr_R'], r['hist_edge_R'] = n, (x.get('sredni_R') if n else None), (rnd(e, 3) if n else None)
        r['hist_t_edge'], r['hist_zrodlo'] = (x.get('t_edge') if x else None), 'test akcji (sesja USA)'
        r['score_R'] = rnd(e * n / (n + RANK_SHRINK_K), 3) if n else 0.0
        if x and x.get('odblokowany'):
            r['filtr'] = ';'.join(f_ for f_ in (r.get('filtr') or '').split(';') if f_ and f_ != 'tradfi_bez_testu')
            r['status_bramki'] = 'DO GRY'
        else:
            r['status_bramki'] = 'OBSERWACJA'
        r['tradfi_test_n'] = n


def squeeze_watch(PP, summary, now_ms, drive_ok):
    """V6.12: alarm odbicia - skrajne wyprzedanie rynku + zebranie dolka 24h BTC z powrotem nad nim. Tylko informacja."""
    if not SQUEEZE_WATCH:
        return None
    rows = sorted([r for r in summary if r.get('tradable') and r.get('asset_type') == 'COIN'],
                  key=lambda r: -(r.get('quote_vol_24h') or 0))[:60]
    shares = []
    for off in range(SQUEEZE_OKNO_H):                      # ostatnie N zamknietych godzin
        low_rsi = n = 0
        for r in rows:
            a = PP.get(r['symbol'], {}).get('arr', {}).get('1h')
            if a is None or len(a) < 60 + off:
                continue
            c = a[:len(a) - off, C][-150:]
            v = rsi(c)
            if v is None:
                continue
            n += 1
            low_rsi += v < 30
        if n:
            shares.append(low_rsi / n)
    breadth_now = shares[0] if shares else None
    breadth_6h = max(shares) if shares else None
    b = PP.get('BTCUSDT', {}).get('arr', {}).get('1h')
    sweep = reclaim = False
    sweep_low = prior_low = None
    if b is not None and len(b) > 30:
        for k in (1, 2):                                   # zebranie w ostatniej lub przedostatniej zamknietej swiecy
            i = len(b) - k
            pl = float(b[i - 24:i, L].min())
            if b[i, L] < pl:
                sweep, sweep_low, prior_low = True, float(b[i, L]), pl
                reclaim = reclaim or bool(b[-1, C] > pl)
    signal = bool(breadth_6h is not None and breadth_6h >= SQUEEZE_BREADTH and sweep and reclaim)
    cand = []
    if signal:
        ryz = KAPITAL_USDT * RYZYKO_NA_TRANSAKCJE_PCT / 100 * SQUEEZE_RYZYKO_R
        pool = [r for r in summary if r.get('tradable') and r.get('asset_type') == 'COIN' and (r.get('vol_rank') or 999) <= SQUEEZE_MAX_RANK
                and r['symbol'] != 'BTCUSDT' and (str(r.get('struct_15m', '')).startswith('HH') or '+' in str(r.get('last_event_15m', '')))]
        for r in sorted(pool, key=lambda r: -(r.get('rs_btc_24h') or -99)):
            if len(cand) >= 5:
                break
            a1 = PP[r['symbol']]['arr'].get('1h')
            if a1 is None or len(a1) < 24:
                continue
            price = float(r['live_price'])
            sl = float(a1[-24:, L].min()) * 0.998          # pod dolkiem 24h (badanie: ciasny SL pod ostatnim swingiem byl wybijany)
            if sl >= price or (price - sl) / price * 100 > SQUEEZE_MAX_SL_PCT:
                continue
            ilosc = ryz / (price - sl)
            cand.append({'symbol': r['symbol'], 'cena': price, 'sl': psig(sl, 6), 'ryzyko_pct': rnd((price - sl) / price * 100, 2),
                         'plus_1R': psig(price + (price - sl), 6), 'ilosc': psig(ilosc, 4), 'ryzyko_usdt': rnd(ryz, 2),
                         'rs_btc_24h': r.get('rs_btc_24h'), 'struktura_15m': r.get('struct_15m')})
    out = {'alarm_odbicia': signal, 'wyprzedanie_teraz': rnd(breadth_now, 2), 'wyprzedanie_6h_max': rnd(breadth_6h, 2),
           'btc_zebral_dolek_24h': sweep, 'btc_powrot_nad_dolek': reclaim, 'btc_dolek': sweep_low, 'btc_poprzedni_dolek_24h': prior_low,
           'prog': SQUEEZE_BREADTH, 'kandydaci_long': cand}
    try:
        row = {'snapshot_utc': snap_iso(now_ms), **{k: v for k, v in out.items() if k != 'kandydaci_long'},
               'btc': float(b[-1, C]) if b is not None and len(b) else None, 'kandydaci': ';'.join(c['symbol'] for c in cand)}
        df = pd.DataFrame([row])
        df.to_csv(OUT / 'squeeze.csv', index=False)
        if drive_ok and not REPLAY_ZIP:
            fp = Path(DRIVE_DIR) / SQUEEZE_LOG
            df.to_csv(fp, mode='a', header=not fp.exists(), index=False)
    except Exception as exc:
        print(f'squeeze log: blad ({str(exc)[:100]})')
    return out


DZIENNIK_KOLUMNY = ['data_pl', 'symbol', 'side', 'wynik_R', 'pnl_usdt', 'zrodlo', 'setup', 'uwagi']


def journal_day(drive_ok, now_ms):
    """V6.11: wynik dnia (czas PL) z dziennika na Drive albo z WYNIK_DNIA_R + podsumowanie dziennika do zipa."""
    t = datetime.fromtimestamp(now_ms / 1000, tz=timezone.utc)
    if ZoneInfo is not None:
        try:
            t = t.astimezone(ZoneInfo('Europe/Warsaw'))
        except Exception:
            pass
    dzis = t.strftime('%Y-%m-%d')
    out = {'data_pl': dzis, 'wynik_R': None, 'transakcje': 0, 'zrodlo': None, 'limit_R': DZIENNY_LIMIT_R,
           'limit_osiagniety': False}
    try:
        pd.DataFrame(columns=DZIENNIK_KOLUMNY).to_csv(OUT / 'dziennik_wzor.csv', index=False)
    except Exception:
        pass
    df = None
    fp = Path(DRIVE_DIR) / DZIENNIK_CSV
    if drive_ok and fp.exists():
        try:
            df = pd.read_csv(fp)
        except Exception as exc:
            print(f'{DZIENNIK_CSV}: blad odczytu ({str(exc)[:100]})')
    elif Path(DZIENNIK_CSV).exists():
        try:
            df = pd.read_csv(DZIENNIK_CSV)
        except Exception:
            df = None
    if df is not None and len(df):
        ryz = KAPITAL_USDT * RYZYKO_NA_TRANSAKCJE_PCT / 100
        r = pd.to_numeric(df.get('wynik_R'), errors='coerce') if 'wynik_R' in df else pd.Series(np.nan, index=df.index)
        if 'pnl_usdt' in df:
            r = r.fillna(pd.to_numeric(df['pnl_usdt'], errors='coerce') / ryz)
        df['R'] = r
        df['zrodlo'] = df.get('zrodlo', pd.Series('?', index=df.index)).fillna('?').astype(str).str.lower()
        df.to_csv(OUT / 'dziennik.csv', index=False)
        groups = [('zrodlo', ['zrodlo'])] + ([('setup', ['setup'])] if 'setup' in df else [])
        rows = []
        for name, cols in groups:
            for key, g in df.dropna(subset=['R']).groupby(cols):
                rr = g['R']
                rows.append({'grupa': name, 'wartosc': key[0] if isinstance(key, tuple) else key, 'transakcje': len(rr),
                             'suma_R': rnd(rr.sum(), 2), 'sredni_R': rnd(rr.mean(), 3),
                             'skutecznosc_pct': rnd((rr > 0).mean() * 100, 1)})
        if rows:
            pd.DataFrame(rows).to_csv(OUT / 'dziennik_podsumowanie.csv', index=False)
        d = df[df.get('data_pl', pd.Series('', index=df.index)).astype(str).str[:10] == dzis]
        out.update(wynik_R=rnd(float(d['R'].sum()), 2) if len(d) else 0.0, transakcje=int(len(d)), zrodlo='dziennik')
    if WYNIK_DNIA_R is not None:
        out.update(wynik_R=float(WYNIK_DNIA_R), zrodlo='WYNIK_DNIA_R')
    if out['wynik_R'] is not None and out['wynik_R'] <= -abs(DZIENNY_LIMIT_R):
        out['limit_osiagniety'] = True
    if out['zrodlo']:
        print(f"Wynik dnia {dzis}: {out['wynik_R']}R ({out['transakcje']} tr., {out['zrodlo']})"
              f"{' -> DZIENNY LIMIT STRATY: nic do handlu' if out['limit_osiagniety'] else ''}")
    return out


def apply_conditions(rows, shock, ochrona, dzien=None):
    """Twardy filtr warunkow handlu (nie zmienia oceny): do_handlu True/False + powody."""
    glob = []
    if dzien and dzien.get('limit_osiagniety'):
        glob.append(f"dzienny limit straty ({dzien.get('wynik_R')}R <= -{DZIENNY_LIMIT_R}R)")
    if shock and shock.get('alarm'):
        glob.append('alarm BTC')
    if ochrona and ochrona.get('aktywny'):
        glob.append('tryb ochronny')
    for r in rows:
        why = list(glob)
        s = 1 if r['side'] == 'LONG' else -1
        st = (ochrona or {}).get('strony', {}).get(r['side'], {})
        if OCHRONA_PER_STRONA and st.get('aktywny'):
            why.append(f"tryb ochronny {r['side']} ({st.get('sredni_R')}R / {OCHRONA_DNI}d)")
        if r.get('setup_type') == 'SWING4H':
            if r.get('trend_4h') != s:
                why.append('trend 4h nie zgodny')
        elif r.get('setup_type') == 'CROWD':
            pass                                       # z zalozenia przeciw ruchowi tlumu - bez wymogu trendu
        elif r.get('regime_1h') != 'trend':
            why.append(f"rezim 1h {r.get('regime_1h')}")
        bt = r.get('btc_trend_1h')
        if bt and bt == -s and (r['symbol'] == 'BTCUSDT' or (r.get('corr_btc') or 1) >= BTC_CORR_MIN):
            why.append('BTC 1h przeciw')
        if r.get('ocena') == 'D':
            why.append('ocena D')
        if r.get('status_bramki') != 'DO GRY':
            why.append(f"bramka statystyczna: {r.get('status_bramki', 'OBSERWACJA')} (typ + kierunek + wejscie niepotwierdzone)")
        if r.get('filtr'):
            why.append(f"filtr: {r['filtr']}")
        if not r.get('tradable', True):
            why.append('nie tradable')
        r['do_handlu'] = bool(WARUNKI_HANDLU) and not why
        r['warunki'] = 'OK' if not why else '; '.join(why)
    return glob


def protection_mode(arch, now_ms):
    """Tryb ochronny: sredni wynik setupow po filtrach (bez wylaczonych typow) z archiwum z ostatnich OCHRONA_DNI dni.
    OCHRONA_PER_STRONA: liczony osobno dla LONG i SHORT ('strony'); 'aktywny' = globalny (tylko gdy per strona wylaczone)."""
    empty = {'aktywny': False, 'n': 0, 'sredni_R': None, 'strony': {sd: {'aktywny': False, 'n': 0, 'sredni_R': None}
                                                                     for sd in ('LONG', 'SHORT')}}
    if arch is None or not len(arch):
        return empty
    d = arch[(arch.snapshot_utc >= snap_iso(now_ms - OCHRONA_DNI * DAY_MS)) & arch.status.isin(FINAL)
             & (arch.filtr.fillna('') == '')].copy()
    d = d[~d.apply(lambda x: (x['setup_type'], x['side']) in WYLACZONE_TYPY, axis=1)] if len(d) else d
    if not len(d):
        return empty
    d['r'] = pd.to_numeric(d['r'], errors='coerce')
    d = d[d.r.notna()]
    r = d.r
    act = len(r) >= OCHRONA_MIN_N and r.mean() < 0
    strony = {}
    for sd in ('LONG', 'SHORT'):
        x = d[d.side == sd].r
        strony[sd] = {'aktywny': bool(len(x) >= OCHRONA_MIN_N and x.mean() < 0), 'n': int(len(x)),
                      'sredni_R': rnd(x.mean(), 3) if len(x) else None}
    return {'aktywny': bool(act) and not OCHRONA_PER_STRONA, 'n': int(len(r)), 'sredni_R': rnd(r.mean(), 3) if len(r) else None,
            'globalnie_ujemny': bool(act), 'strony': strony}


def ochrona_strony(ochrona, side):
    """Czy tryb ochronny dotyczy tego kierunku (globalny albo per strona)."""
    if not ochrona:
        return False
    return bool(ochrona.get('aktywny') or (OCHRONA_PER_STRONA and ochrona.get('strony', {}).get(side, {}).get('aktywny')))


def lev_line(r):
    if r.get('dzwignia') is None:
        return 'dzwignia: ' + (r.get('dzwignia_uwagi') or 'brak')
    txt = (f"dzwignia {r['dzwignia']}x (pref {r['dzwignia_pref']}x, bezp. max {r['dzwignia_bezp']}x) | wartosc {r['wartosc_usdt']} USDT, "
           f"depozyt {r['depozyt_usdt']} USDT ({r['depozyt_pct']}%), ilosc {r['ilosc']} | likw. {r['likwidacja']} "
           f"({r['likw_vs_sl']}x SL) | SL -{r['strata_sl_usdt']} / TP1 +{r['zysk_tp1_usdt']} USDT")
    if r.get('tp_1R'):
        txt += (f" | {int(CZESC_1R * 100)}% przy 1R {r['tp_1R']}, potem SL na wejscie" +
                (f" i trailing {TRAIL_PO_1R_R}R za najlepsza cena" if TRAIL_PO_1R_R else '') +
                (f", trzymaj do {HOLD_PO_1R_H}h" if HOLD_PO_1R_H else ''))
    return txt + (f" | UWAGA: {r['dzwignia_uwagi']}" if r.get('dzwignia_uwagi') else '')


def print_report(good, live, bt_df, arch, gate, meta, top=(), htab=None):
    print('\n' + '=' * 90)
    print(f"{VERSION} | {meta['server_time'][:16]} UTC | kontrakty {meta['symbols']} | setupy {len(good)} (wszystkie {len(live)})")
    if meta.get('btc_context'):
        b = meta['btc_context']
        print(f"BTC: trend 1h {b.get('trend_1h')}, 4h {b.get('trend_4h')}, 24h {b.get('chg_24h_pct')}%, rezim {b.get('regime_1h')}")
    sh = meta.get('alarm_btc') or {}
    if sh.get('alarm'):
        print(f"\n!!! ALARM BTC: {sh['od']} -> {sh['teraz']} w {ALARM_BTC_MIN} min ({sh['zmiana_atr15']:+} ATR 15m, zakres {sh['zakres_atr15']} ATR). "
              f"Nie otwieraj nowych pozycji, sprawdz SL na otwartych, poczekaj na zamkniecie swiecy 15m.")
    oc = meta.get('tryb_ochronny') or {}
    if OCHRONA_PER_STRONA:
        for sd, st in (oc.get('strony') or {}).items():
            if st.get('aktywny'):
                print(f"\n!!! TRYB OCHRONNY {sd}: ostatnie {OCHRONA_DNI} dni sr. {st['sredni_R']}R (n={st['n']}) -> {sd} nie do handlu, ryzyko x{OCHRONA_MNOZNIK}")
            else:
                print(f"Tryb ochronny {sd}: nie (ostatnie {OCHRONA_DNI} dni: sr. {st.get('sredni_R')}R, n={st.get('n')})")
    elif oc.get('aktywny'):
        print(f"\n!!! TRYB OCHRONNY: setupy z ostatnich {OCHRONA_DNI} dni sr. {oc['sredni_R']}R (n={oc['n']}) -> ryzyko x{OCHRONA_MNOZNIK} "
              f"(plan dzwigni juz przeliczony)")
    else:
        print(f"\nTryb ochronny: nie (ostatnie {OCHRONA_DNI} dni: sr. {oc.get('sredni_R')}R, n={oc.get('n')}; wymagane n>={OCHRONA_MIN_N})")
    for sd, e in (meta.get('ekspozycja') or {}).items():
        if e.get('za_duze'):
            print(f"!!! EKSPOZYCJA {sd}: {e['setupy_AB']} setupow A/B + {e['pozycje']} pozycji = {e['ryzyko_pct']}% kapitalu "
                  f"(> {MAX_RYZYKO_KIERUNEK_PCT}%) - to jeden zaklad na kierunek rynku, wybierz najlepsze")
            print(f"    miesci sie w limicie: {', '.join(e.get('wybrane') or []) or 'nic (limit zajety przez otwarte pozycje)'}")
    if meta.get('pozycje'):
        print('\nOTWARTE POZYCJE (reguly zarzadzania - jeszcze bez testu wstecznego):')
        for p in meta['pozycje']:
            extra = (f" -> zamknij {p['zamknij_pct']}%" if p.get('zamknij_pct') else '') + \
                    (f" | nowy SL {p['nowy_sl']}" if p.get('nowy_sl') else '')
            print(f"  {p['symbol']:14s} {p['side']:5s} {p['akcja']:13s}{extra} | cena {p.get('price')} wejscie {p.get('entry')} "
                  f"SL {p.get('sl')} | {p.get('R_teraz')}R (maks {p.get('MFE_R')}R)"
                  + (f" | zamknij najpozniej {p['zamknij_najpozniej_pl']}" if p.get('zamknij_najpozniej_pl') else ''))
            print(f"     {p.get('powody')}" + (f" | {p['uwagi']}" if p.get('uwagi') else ''))
    print('\nBRAMKA (dane od', gate.od.iloc[0] if len(gate) else GATE_FROM_UTC, '):')
    for g in gate.itertuples():
        print(f'  {g.typ:16s} {g.status:10s} transakcji {g.transakcje:4d}  sr. R {g.sredni_R}  przewaga {g.sredni_edge_R} '
              f't dzienny {g.t_edge_dzienny}  (brakuje {g.brakuje_transakcji} trans., {g.brakuje_zdjec} zrzutow, {g.brakuje_dni} dni) | bez filtrow: {g.bez_filtrow_transakcje} tr., sr. R {g.bez_filtrow_sredni_R}')
    if htab is not None and len(htab):
        print(f"\nTEST HISTORYCZNY ({HIST_DAYS} dni, {(meta.get('test_historyczny') or {}).get('meta', {}).get('source')}, po filtrach)"
              f" - przewaga = wynik setupu minus losowe wejscie w tym samym kierunku:")
        for h in htab.sort_values('t_edge', ascending=False, na_position='last').itertuples():
            print(f'  {h.typ:16s} {h.side:5s} n={h.transakcje:5d}  sr R {h.sredni_R:+.3f}  losowe {h.losowe_R}  '
                  f'przewaga {h.sredni_edge_R}  t {h.t_edge}  zdjec {h.zdjecia}')
        ok = meta.get('_hist_ok')
        if ok is not None and len(ok):
            lq = liq_compare(ok)
            print(f"  TP na klastrze likwidacji vs zwykly TP1 (te same transakcje, n={lq['liq_n']}): "
                  f"{lq['liq_R_klaster']} vs {lq['liq_R_tp1']} R, t roznicy {lq['liq_t_roznicy']}")
    if len(bt_df):
        s = bt_summary(bt_df)
        print('\nTEST WSTECZNY (ten przebieg):')
        for k, v in s.items():
            if v.get('transakcje'):
                print(f"  {k:28s} n={v['transakcje']:4d}  trafn {v['skutecznosc_pct']:5.1f}%  sr R {v['sredni_R']:+.3f}  "
                      f"przewaga {v.get('sredni_edge_R')}  t {v.get('t_edge')}  zdjec {v['zdjecia']}")
    for mode in ('MARKET', 'LIMIT'):
        rows = [t for t in top if t['mode'] == mode]
        print(f'\nTOP {mode} (przewaga nad losowym wejsciem z t >= {TOP_MIN_T}; max {TOP_MAX_PER_SIDE} na kierunek, 1 na kontrakt):')
        if not rows:
            print('  brak - zaden obecny setup z ocena A-C nie ma potwierdzonej przewagi w historii')
        for t in rows:
            print(f"  {t['rank']}. [{t['status_bramki']}] {t['symbol']:14s} {t['side']:5s} {t['setup_type']:10s} wejscie {t['entry']} "
                  f"({t['dist_entry_pct']:+.2f}%) SL {t['sl']} TP1 {t['tp1']} RR {t['rr_tp1_net']} | ocena {t['score_R']:+.3f}R "
                  f"(n={t['hist_n']}, sr {t['hist_sr_R']:+.2f}R, przewaga {t['hist_edge_R']:+.2f}R t {t['hist_t_edge']}, {t['hist_zrodlo']})")
            print('     ' + lev_line(t))
    rk = meta.get('ranking') or []
    bb = meta.get('btc_bias_5m')
    bt1 = (meta.get('btc_context') or {}).get('trend_1h')
    wg = meta.get('warunki_globalne') or []
    print(f"\nWARUNKI DO HANDLU: {'NIE - ' + ', '.join(wg) if wg else 'globalnie OK (sprawdz kolumne DO HANDLU przy setupie)'}")
    sq = meta.get('squeeze') or {}
    if sq:
        print(f"\nSQUEEZE WATCH (V6.12, tylko informacja): wyprzedanie teraz {sq.get('wyprzedanie_teraz')}, max {SQUEEZE_OKNO_H}h {sq.get('wyprzedanie_6h_max')} "
              f"(prog {sq.get('prog')}) | BTC zebral dolek 24h: {sq.get('btc_zebral_dolek_24h')}, powrot nad dolek: {sq.get('btc_powrot_nad_dolek')}")
        if sq.get('alarm_odbicia'):
            print(f"  >>> ALARM ODBICIA: rynek skrajnie wyprzedany i BTC wrocil nad zebrany dolek {sq.get('btc_poprzedni_dolek_24h')}. "
                  f"Sygnal NIEUDOWODNIONY (8 zdarzen w 40 dniach) - najwyzej {SQUEEZE_RYZYKO_R}R, 1 pozycja:")
            for c_ in sq.get('kandydaci_long') or []:
                print(f"     LONG {c_['symbol']:14s} cena {c_['cena']} SL {c_['sl']} ({c_['ryzyko_pct']}%) +1R {c_['plus_1R']} "
                      f"ilosc {c_['ilosc']} (ryzyko {c_['ryzyko_usdt']} USDT) | RS vs BTC {c_['rs_btc_24h']} | 15m {c_['struktura_15m']}")
    if TRADFI_ON:
        tf_rows = [r for r in (meta.get('ranking') or []) if r.get('asset_type') in TRADFI_TYPES]
        tt = meta.get('tradfi_test') or []
        odb = [x for x in tt if x.get('odblokowany')]
        print(f"\nTRADFI (V6.14: akcje, indeksy, metale, surowce; sesja USA {'OTWARTA' if meta.get('us_session_open') else 'ZAMKNIETA'}):")
        print(f"  test akcji/indeksow (sesja USA, archiwum): {sum(int(x.get('transakcje') or 0) for x in tt)} transakcji, "
              f"odblokowane typy: {', '.join(x['typ_wersja'] + ' ' + x['side'] for x in odb) or 'brak (filtr tradfi_bez_testu -> D)'}")
        print('  metale i surowce: ten sam test i bramka co krypto (bez zmian)')
        for mode in ('MARKET', 'LIMIT'):
            rows = [r for r in tf_rows if r['entry_type'] == mode][:5]
            print(f'  {mode}:')
            if not rows:
                print('    brak setupow')
            for r in rows:
                print(f"    [{r['ocena']}] {'DO HANDLU' if r.get('do_handlu') else 'czekaj':9s} {r['symbol']:12s} {r.get('asset_type', ''):11s} {r['side']:5s} "
                      f"{r['setup_type']:10s} wejscie {r['entry']} ({r['dist_entry_pct']:+.2f}%) SL {r['sl']} TP1 {r['tp1']} RR {r['rr_tp1_net']} "
                      f"ryzyko {r['risk_pct']}%" + (f" | filtr: {r['filtr']}" if r.get('filtr') else ''))
    gt = meta.get('gate_tlum') or {}
    if gt.get('kontrakty'):
        def _g(f):
            return (f"L/S konta {f.get('gate_lsr_konta')} (z {f.get('gate_lsr_z')}), taker {f.get('gate_lsr_taker')}, "
                    f"likw. 4h L {f.get('gate_likw_long_4h_usd')} / S {f.get('gate_likw_short_4h_usd')} USD") if f else 'brak'
        print(f"\nTLUM Z GATE.IO (V6.13, inna gielda, tylko informacja; {gt['kontrakty']} kontraktow):")
        print(f"  BTC: {_g(gt.get('BTCUSDT'))}")
        print(f"  ETH: {_g(gt.get('ETHUSDT'))}")
        print(f"  likwidacje 4h razem: long {gt.get('likw_long_4h_usd')} USD, short {gt.get('likw_short_4h_usd')} USD "
              f"(udzial long {gt.get('likw_long_udzial')}) - przewaga likwidacji long = wyplukani kupujacy, short = wyciskane shorty")
    print(f"\nRANKING Z OCENA (A najlepsze, D pomin; BTC trend 1h: {'w gore' if bt1 == 1 else 'w dol' if bt1 == -1 else 'brak'}, "
          f"BTC 5m: {'w gore' if bb == 1 else 'w dol' if bb == -1 else 'brak'}):")
    for mode in ('MARKET', 'LIMIT'):
        rows = [r for r in rk if r['entry_type'] == mode][:OCENA_TOP_N]
        print(f'  {mode}:')
        if not rows:
            print('    brak setupow')
        for r in rows:
            print(f"    [{r['ocena']}] {'DO HANDLU' if r.get('do_handlu') else 'czekaj':9s} {r['ocena_pkt']:+d} {r['symbol']:14s} {r['side']:5s} {r['setup_type']:10s} wejscie {r['entry']} "
                  f"({r['dist_entry_pct']:+.2f}%) SL {r['sl']} TP1 {r['tp1']} RR {r['rr_tp1_net']} ryzyko {r['risk_pct']}% "
                  f"dzw. {r.get('dzwignia')}x (max {r.get('dzwignia_bezp')}x)" + (f" | filtr: {r['filtr']}" if r.get('filtr') else ''))
            print(f"         {r['ocena_powody']}" + ('' if r.get('do_handlu') else f" | warunki: {r.get('warunki')}"))
            if r.get('gate_lsr_konta') is not None:
                print(f"         Gate: L/S {r['gate_lsr_konta']} (z {r.get('gate_lsr_z')}), likw. 4h L {r.get('gate_likw_long_4h_usd')} / "
                      f"S {r.get('gate_likw_short_4h_usd')} USD, OI 4h {r.get('gate_oi_chg_4h_pct')}% | WEEX OI 4h {r.get('oi_chg_4h_pct')}%")
        if rows and all(r['ocena'] == 'D' for r in rows):
            print('    -> nic wartego wejscia w tym trybie')
    print('\nSETUPY (po filtrach, tradable):')
    if not good:
        print('  brak')
    for mode, title in (('MARKET', 'WEJSCIE PO RYNKU (teraz)'), ('LIMIT', 'ZLECENIA LIMIT (czekaja na cene)')):
        rows = sorted([r for r in good if r['entry_type'] == mode],
                      key=lambda r: ('ABCD'.index(r.get('ocena') or 'D'), -(r.get('ocena_pkt') or 0)))
        print(f'  {title}: {len(rows)}')
        if good and not rows:
            print('    brak')
        for r in rows[:15]:
            print(f"    [{r.get('ocena', '?')}] [{r['status_bramki']}] {r['symbol']:14s} {r['side']:5s} {r['setup_type']:10s} wejscie {r['entry']} "
                  f"({r['dist_entry_pct']:+.2f}%) SL {r['sl']} TP1 {r['tp1']} RR {r['rr_tp1_net']} koszt {r['cost_R']}R | {r['trigger']}")
            print('       ' + lev_line(r) + f" | {r['cancel_if']}")
    if any(r['status_bramki'] == 'OBSERWACJA' for r in good):
        print('\n  OBSERWACJA = typ jeszcze bez potwierdzonej skutecznosci na nowych danych. Nie traktuj tego jak sygnalu.')


if __name__ == '__main__':
    main()
