# V6.183: naprawa UnboundLocalError drive_ok/replay; snapshot OHLCV po mount_drive, przed aktualizacja.
# V6.182: porownanie OHLCV po timestamp przed/po aktualizacji Drive; tylko diagnostyka.
# V6.178: QC compact delta + available input candle provenance, observational only; no execution attribution.
# V6.177: diagnostyka QC porownan, uczestnictwa w raportach i manifest wersji; bez zmian handlu.
# V6.174: wewnetrzne DataFrame to_dict(records): identyczne kolumny scalane, konflikt fail-closed; bez zmian handlu.
# WEEX V6.173: uruchom przez osobny plik launcher_colab_v173.py, aby zapisac SHA-256 w manifeście.
# WEEX V6.172: rozpoznawalny manifest pliku + bez deprecated groupby.apply.
# Colab, jedna komorka po wgraniu pliku:
# import os, hashlib, runpy
# from pathlib import Path
# p=Path('/content/weex_v6_172.py').resolve()
# digest=hashlib.sha256(p.read_bytes()).hexdigest()
# os.environ.update(WEEX_EXECUTED_FILE=str(p), WEEX_EXPECTED_SHA256=digest, WEEX_LAUNCH_SHA256=digest)
# runpy.run_path(str(p), run_name='__main__')
# UWAGA: hash potwierdza bajty pliku, nie jest niezaleznym dowodem wykonania.
# V6.169: independent slot audit respects half-open [entry,exit) and zero-duration entries; verifies each selected signal against prior active reservations.
# V6.168: fix zero-duration positions in independent slot sweep (exit == entry); tests preserve cap and reconciliation.
# V6.167: independent event audit, warmup vs OOS, continuous timeline slot checks. Research only.
# V6.166: continuous chronological Elliott ABC research portfolio, exact occupied slots at WF cuts, carry-over regression. No live gate change.
# V6.165: event-driven ABC research portfolio, carry-over across WF folds; no active gate changes.
# V6.163: paired walk-forward comparison of selected cost/R threshold vs unfiltered Elliott ABC.
# V6.162: badawcze progi koszt/R, deduplikacja ABC miedzy siatkami i chronologiczna walidacja z odcieciem exit_utc. Bez zmian handlu.
# V6.161 (29.10): poprawne jednostki bps w diagnostyce koszt/R ABC;
# osobne kandydatury/rozliczone/otwarte/oczekujace/inne i kontrola sum grup.
# Wyłącznie raporty badawcze: bez zmian simulate(), gate, PROBA i do_handlu.
# V6.160: diagnostyka koszt/R, oddzielne COIN/TradFi i siatki ABC 12h/6h/3h (bez zmian bramek).
# V6.158: Elliott kontrola geometrii SL, odrebny status ABC i unikalne okazje TOP, raport diagnostyczny, bez zmian handlu.
# V6.155: parowane odtworzenie statystyk bramki na ocenialnym replayu 7d; bez zmiany gate produkcyjnego.
# V6.154: naprawa testu replay A/B: swiece w faktycznym ukladzie O,H,L,C,V,QV,TB,T (timestamp w kolumnie 7).
# V6.152: diagnostyka nog kosztowych i hipotetycznych zmian bramki przy 30 bps; bez replay fill/exit i zmian decyzji.
# V6.151: rownolegle zestawienie wyniku obecnego modelu kosztu 5/10/12/15 bps ze stalym 30 bps
#   na tych samych zrealizowanych zdarzeniach (bez ponownego symulowania fill/exit).
#   Roznica R jest szacowana wedlug znanego modelu oplat; zaokraglenie R = 0.001.
#   Bramka i dane handlowe bez zmian, raport ma charakter badawczy.
# V6.150 (29.10): poslizg w tescie wstecznym wg wolumenu 24h w chwili zdjecia (5/10/12 bps, brak danych 15) zamiast
#   stalych 30 bps z V6.136 - 30 bps zanizalo kazdy wynik o ~0.15R (bramka, tryb ochronny, hist_*). Bez lookahead booka.
# V6.130: slady rozliczen i identyfikacja zmian historycznych Elliott OOS; bez zmian silnika.
# V6.127: trwaly audyt par i zmian portfela Elliott OOS oraz kontrola zgodnosci R (raporty tylko diagnostyczne).
# V6.126: naprawa KeyError setup_type w tescie Elliott v681 (opcjonalna kolumna).
# V6.125: audyt wybranych transakcji portfela, koncentracja wyniku i zmiany par OOS.
# V6.124: porownanie decyzji MARKET/LIMIT (takze brak fill), portfel chronologiczny i werdykt OOS.
# V6.123: parowany MARKET/LIMIT OOS, strategie, dzienne wyniki i stres kosztowy; bez zmian decyzji.
# V6.122: osobny raport badawczy OOS MARKET / LIMIT / swieze ABC; bez zmian handlu.
# V6.121: historyKlines limit<=100 wedlug WEEX; diagnostyka swiezosci ignoruje otwarta swiece.
# V6.120: US30 1h: historyKlines startTime/endTime, bez bezcelowych powtorzen /klines; fail-closed przy braku pokrycia.
# V6.119: porownanie US30 1h z kontrola pokrycia i dodatkowa proba historyczna; jawne ID setupu.
# V6.117: diagnostyka zdarzen zerowych OHLCV, nakladanie przedzialow TF i zgodnosc API/archiwum; bez zmian handlu.
# V6.116: diagnostyka zera OHLCV API/archiwum i przedzialow czasowych, bez zmian decyzji.
# V6.115: diagnostyka odrzucanych swiec z rozbiciem na przyczyny i probkami (bez zmian filtrow).
# V6.114: odrzucenia przypisane per interwal, status PIERWSZE/POWTARZA_SIE i osobna lista powtarzalnych; tylko raporty.
# V6.113: diagnostyka powtarzalnych bledow swiec i porownanie QC miedzy uruchomieniami; bez zmian logiki handlu.
# V6.112 (28.10): poprawne blokady per interwal w historia_problemy, swiezosc i ciaglosc; odrzucone swiece = ostrzezenie, nie blokada samo w sobie.
# V6.111: diagnostyka brakow historii, potwierdzona krotka historia i kontrola wynikow napraw.
# V6.110: diagnostyka naturalnie krotkiej historii oraz ograniczenie bezcelowego pelnego pobierania.
# V6.109: kontrola luk archiwum i naprawa brakujacych swiec bez oslabiania swiezosci.
# V6.108: 16 watkow przy niezmienionym limicie WEEX 10 req/s; diagnostyka opoznien, ponowien i odczytow OI/depth.
# V6.107: diagnostyka uzupelniania swiec, bezpieczny cache fundingu z kopia i odzyskiwaniem; bez zmian strategii.
# V6.106: poprawna klasyfikacja opoznionych ABC takze gdy pola swiezosci znikna w pipeline;
# brak danych o odleglosci NIE oznacza swiezosci. Osobne rankingi wysokiej jakosci A-C.
# V6.105: rozdzielona jakosc/wykonalnosc, opoznione ABC poza TOP, diagnostyka cache.
# V6.103: ABC/Fibonacci research, oddzielne wyniki MARKET/LIMIT/ELLIOTT. Elliott nie omija bramek.
# V6.104: rankingi MARKET/LIMIT/ELLIOTT (DO GRY/PROBA vs OBSERWACJA), ABC: swiezosc od C.
# V6.118 (28.10): kontrolowany odczyt zer US30 1h/4h na identycznych znacznikach UTC; bez dopisywania cen.
# V6.102 (28.10): liczniki trafien pamieci w run_meta tylko z glownego testu 7 dni (test 30 dni je zerowal).
# V6.101 (27.10): pamiec testu wstecznego dziala miedzy sesjami Colab i przezywa zmiany raportow:
#  - odcisk kodu byl inny w kazdej nowej sesji (kolejnosc frozenset w stalych zalezy od losowego ziarna Pythona),
#    wiec plik pamieci z Drive byl zawsze odrzucany ('inny kod') - teraz zbiory sortowane, odcisk stabilny;
#  - odcisk obejmuje tylko funkcje i stale osiagalne z analyze() - nowa wersja z samymi raportami nie kasuje pamieci.
# V6.100: trwala diagnostyka API oraz skutecznosci i stanu cache; bez zmian sygnalow i ryzyka.
# V6.99: bezpieczny cache JSON+zlib, diagnostyka API per endpoint.
# V6.96: przywrocone pelne raporty Elliott, portfolio i OOS; optymalizacje HTTP zachowane.
# =====================================================================================
# V6.94: szybsza kolejka HTTP (sleep poza lock), 10 workers przy tym samym
# limicie 10 req/s, rownolegle retry 5m bez powtarzania OI/funding/depth.
# Test historyczny, zakres kontraktow i zabezpieczenia handlowe bez zmian.
# =====================================================================================
# WEEX FUTURES V6.95 FAST SCAN - JEDEN PLIK
# Colab (bez kopiowania tekstu): w komorce
#   from google.colab import files; exec(open(list(files.upload())[0], encoding='utf-8').read())
# i wybierz ten plik. Lokalnie: python weex_v6.py
# Wynik: weex_v6_RRRRMMDD_HHMMSS.zip -> top_setups.csv (najlepsze MARKET i LIMIT), setups.csv (po filtrach),
#        hist_summary.csv (test historyczny 30 dni),
#        setups_all.csv (z odrzuconymi),
#        summary.csv/json, backtest.csv, backtest_archive.csv, gate.csv, run_meta.json, qc.json,
#        universe.csv (wszystkie kontrakty WEEX + powod pominiecia), movers.csv (liderzy 24h i dlaczego bez setupu),
#        coins/*.json (surowe dane - pozwalaja odtworzyc analize: REPLAY_ZIP)
#
# V6.98 (27.10 - szybkosc testu wstecznego, BEZ zmian wynikow):
#  - BT_CACHE: wynik analizy (cechy + setupy PRZED symulacja) kazdego kontraktu na kazdym zdjeciu testu 7 dni
#    jest zapamietywany. Zdjecia sa co 2 h w stalej siatce, wiec kolejne uruchomienie liczy od nowa tylko nowe
#    zdjecie i kontrakty, ktorych swiece sie zmienily. Symulacja wyniku (fill/SL/TP), deduplikacja, rankingi
#    i kontrola losowa sa liczone zawsze od nowa (wyniki otwartych setupow sie aktualizuja).
#  - Klucz: kontrakt, zdjecie, suma kontrolna wszystkich okien swiec (i BTC), funding z, oplaty, oraz odcisk
#    kodu (wszystkie funkcje + stale konfiguracji) - zmiana kodu albo danych = liczenie od nowa.
#  - Pamiec: w biezacej sesji Colab (kolejne uruchomienia w tym samym srodowisku) + plik na Google Drive
#    (bt_cache_v698.pkl) dla nowej sesji. BT_CACHE = False wylacza.
#
# V6.97 (27.10 - szybkosc, BEZ zmian wynikow; wszystkie 60 plikow CSV identyczne jak V6.96 w replayu):
#  - elliott_portfel_ab: jedno stabilne sortowanie calosci zamiast sortowania kazdego zdjecia + iterrows,
#    tylko potrzebne kolumny -> raporty badawcze portfela ok. 80 s -> kilka s.
#  - funding_z_at: historia fundingu sortowana raz na kontrakt (cache) + bisect, zamiast przy kazdym zdjeciu.
#
# V6.93 (27.10 - TradFi: przywrocone twarde blokady wykonania i ryzyka):
#  - Bramka krypto, ocena i rezim pozostaja ostrzezeniami dla TradFi.
#  - Twarde: brak plynnosci, spread/book/wolumen, niespojne dane ceny, minimum kontraktu
#    przekraczajace limit ryzyka, zamkniety rynek, dzienny limit i globalny fail-closed.
#  - Pola ostrzezenia_tradfi oraz blokady_wykonania_tradfi, test regresyjny.
#
# V6.92 (27.10 - TradFi bez blokad; decyzja uzytkownika; scalone z V6.91):
#  - TRADFI_BEZ_BLOKAD = True: setupy TradFi (akcje, indeksy, Pre-IPO, metale, surowce) NIE sa blokowane przez
#    ocene D, rezim, BTC 1h przeciw, alarm BTC, tryb ochronny, bramke/PROBA (statystyki krypto), filtry
#    (tradfi_bez_testu, koszt, typ_wylaczony, ...), plynnosc (book/wolumen) ani minimum kontraktu.
#    Te powody trafiaja do kolumny 'ostrzezenia_tradfi' (informacja, nie blokada).
#  - Twarde blokady zostaja tylko: rynek faktycznie zamkniety (martwe swiece, 'TradFi zamkniety'), dzienny limit straty,
#    limit ekspozycji na kierunek, brak Google Drive / blad archiwum (fail closed).
#  - status_setupu = 'BEZ BRAMKI'; TradFi nie wchodzi do PROBA (ani do jej limitu i dziennika); trafia do setups.csv.
#  - ryzyko_mnoznik bez zmian (typ bez zatwierdzenia -> x0.5, tryb ochronny -> mnoznik), czyli mniejsza pozycja.
#
# V6.90 (27.10): dzienna stabilnosc trzech rankingow, bez zmian handlu.
# portfolio_three_way_daily.csv / portfolio_three_way_leave_one_day_out.csv / portfolio_three_way_stability.csv.
# Wynik wg dnia zamkniecia; leave-one-day-out usuwa PnL bez ponownej selekcji.
# Koszty 0/.05/.10R, sloty 2/3, OOS i rozdzielenie retrospektywnych regul.
#
# V6.89 (27.10): wspolny test diagnostyczny BAZA / MARKET_PIERWSZE / ELLIOTT.
# portfolio_three_way.csv: identyczne kandydatury, czasy, ograniczenia slotow i koszty.
# Separacja filtra historycznego i retrospektywnego; brak modyfikacji decyzji do_handlu.
# Raport nie dowodzi przewagi OOS; rozliczenia wg chwili sygnalu, nie daty zamkniecia.
#
# Co nowego w V6.88 (27.10 - akcje/indeksy/Pre-IPO na WEEX 24h):
#  - TRADFI_24H = True: akcje, indeksy i Pre-IPO nie sa juz blokowane poza sesja USA ('TradFi zamkniety' tylko gdy
#    ostatnie 6 swiec 5m nie ma wolumenu albo zakresu - jak dotad dla metali/surowcow). 27.10 SHEIN SHORT LIMIT
#    4.744 oznaczony 'TradFi zamkniety', a na WEEX wypelnil sie i zarabial.
#  - test wsteczny 7 dni: akcje/indeksy na wszystkich zdjeciach (wczesniej tylko sesja USA), z pominieciem
#    zdjec z martwymi swiecami (rynek faktycznie zamkniety, np. weekend).
#  - tradfi_test.csv: dodatkowo wynik osobno W SESJI USA i POZA SESJA (snapshot_utc) - poza sesja nie ma historii
#    sprzed V6.88; filtry plynnosci (spread, ksiazka, wolumen) BEZ zmian.
#
# Co nowego w V6.87 (27.10 - test kolejnosci wyboru w portfelu; ZERO ZMIAN handlu):
#  - portfolio_market_ab.csv: BAZA vs MARKET_PIERWSZE (wejscia po rynku wybierane przed LIMIT, dalej jak BAZA).
#    Dwie populacje: FILTRY_Z_CHWILI_SYGNALU (jak raporty Elliotta) i AKTUALNE_REGULY (bez typow wylaczonych
#    dzisiejsza konfiguracja), okresy: CALOSC, dwie polowy i OOS od 2026-10-27. Normalna i odwrotna kolejnosc remisow.
#  - Powod: w symulacji 11-27.10 niewypelnione LIMIT blokowaly sloty; MARKET_PIERWSZE bylo jedynym wariantem
#    dodatnim w obu polowach (aktualne reguly: +8.8R vs BAZA -9.4R), ale proba jest mala -> tylko obserwacja OOS.
#  - elliott_portfel_ab(..., warianty=...) - domyslnie jak dotad (BAZA, ELLIOTT_HIPOTEZA); wyniki bez zmian.
#
# Co nowego w V6.86 (przyrost OOS miedzy uruchomieniami; tylko diagnostyka):
#  - elliott_oos_delta.csv: nowe sygnaly/okazje/rozliczenia wzgledem poprzedniego snapshotu.
#  - stan OOS zapisany atomowo na Google Drive; uszkodzony lub brakujacy stan -> BRAK_POROWNANIA.
#  - elliott_oos_population.csv: osobne populacje WSZYSTKIE_SYGNALY i PORTFEL_AB.
#  - brak zmiany regul handlu, filtrow, ENTRY, SL, TP i PROBA.
#
# Co nowego w V6.85 (diagnostyka OOS, bez zmian handlu):
#  - elliott_oos_independence.csv: klastry zdjec 1h, unikalne okazje, ostrzezenie o skupieniu obserwacji.
#  - elliott_oos_phase_results.csv: wyniki rozliczonych sygnalow z rozpoznana faza vs BRAK/NIEOBLICZONE.
#  - elliott_oos_alert.csv: jawne ostrzezenie przy ujemnym OOS i niewystarczajacej probie.
#  - nie odblokowuje handlu, nie zmienia entry/SL/TP ani PROBA.
#
# Co nowego w V6.84 (wyłącznie diagnostyka OOS):
#  - elliott_ab_verdict.csv / _oos.csv: jawny status BRAK_POTWIERDZONEJ_PRZEWAGI
#    dla wynikow ujemnych, slabych prob, braku odpornosci dziennej i kosztowej.
#  - elliott_oos_progress.csv: niezalezne okazje, zdjecia UTC, dni rozliczen;
#    osobno udzial rozpoznanych faz Elliottta i brakujacych oznaczen.
#  - nie odblokowuje transakcji i nie zmienia do_handlu.
#
# Co nowego w V6.83 (leave-one-day-out calego okresu, tylko raporty):
#  - elliott_ab_leave_one_day_out.csv oraz _oos.csv: kazda data zamkniecia usuwana
#    osobno i sparowanie z BAZA, wraz z wplywem na przewage, rankingiem sesji.
#  - elliott_ab_stability.csv oraz _oos.csv: ile dni Elliott ma przewage,
#    liczba dni odwracajacych przewage po usunieciu i liczba dni z transakcjami.
#  - brak modyfikacji entry/SL/TP, bramki ani decyzji do_handlu.
#
# Co nowego w V6.82 (diagnostyka odpornosci wynikow, BEZ ZMIAN handlu):
#  - elliott_ab_robustness.csv: wynik bazowy, osobno bez najlepszego dnia kazdego wariantu,
#    oraz bez najlepszego dnia przewagi Elliott-BAZA (ten sam dzien usuniety w obu).
#  - elliott_ab_cost_verdict.csv: zysk bezwzgledny wariantu Elliott i przewaga wzgledna
#    przy 2/3 miejscach i koszcie 0 / 0.05 / 0.10 R; tylko ocena badawcza.
#  - odpowiedniki OOS bez retrospektywnego dopisywania starych transakcji.
#
# Co nowego w V6.81 (raporty badawcze; bez zmian decyzji handlowych):
#  - A/B: stan oczekujacy LIMIT -> wypelniony -> zamkniety / wygasly na podstawie czasow zdarzen;
#    kontrola spoznionego fill po TTL, bez wyboru na podstawie przyszlego wyniku.
#  - elliott_portfolio_ab_daily.csv: PnL wg dnia zamkniecia, z zerowymi dniami kalendarzowymi.
#  - elliott_oos_daily_quality.csv: koncentracja PnL, liczba dni, najlepszy dzien, zaleznosc od dnia.
#  - elliott_portfolio_ab_oos_daily.csv: OOS wedlug dnia wyjscia.
#  - Jedna aktywna pozycja / oczekujacy LIMIT na symbol; limit kierunku nadal badawczy.
#
# Co nowego w V6.79 (raporty badawcze; ZERO ZMIAN handlu):
#  - A/B wg daty zamkniecia: elliott_portfolio_ab_exit_weekly.csv (nie przypisuje zysku do tygodnia sygnalu).
#  - stres-test A/B: miejsca=2/3 oraz dodatkowe 0.05/0.10R na wykonana transakcje.
#  - zachowanie wersji wynikow OOS od 2026-10-27: rozroznienie proby projektowej od pozniejszych obserwacji.
#  - gate.csv: informacyjne pola typ_wylaczony_konfiguracja oraz status_operacyjny_typu.
#    (poprawka: rodzaj wejscia z sufiksu '.M', zeby CONT_SWEEP.v4 LONG LIMIT pokazywal sie jako WYLACZONY)
#
# Co nowego w V6.78 (diagnostyczne A/B; zasady handlu BEZ ZMIAN):
#  - oczekujacy LIMIT rezerwuje miejsce tylko do terminu waznosci zlecenia, otwarta pozycja do konca danych;
#    nieznany wynik LIMIT po TTL nie jest uznawany za wykonany.
#  - osobny raport elliott_portfolio_ab_weekly.csv: tydzien UTC, wybrane zlecenia, rozliczone, suma R, win rate.
#  - test regresyjny wygaśnięcia LIMIT i trwania otwartej pozycji.
#
# Co nowego w V6.77 (chronologiczne A/B bez selekcji po przyszlym fill):
#  - wybor kandydatow przy snapshot_utc bez filtra status/fill/exit; rezerwacja slotow dla LIMIT do wygasniecia,
#    wykonanie i wynik odczytywane DOPIERO po wyborze. Nieudane LIMIT nie sa liczone jako transakcje.
#  - test przypadku: najwyzszy ranking niewypelniony LIMIT blokuje nizszy ranking na tym samym zdjeciu.
#  - A/B nadal tylko diagnostyczny: brak pelnej rekonstrukcji live, depozytu i egzekucji gieldowej.
#  - Poprawka: status otwarty/oczekuje (wynik jeszcze nieznany) trzyma slot do konca danych i jest liczony
#    w kolumnie w_toku - wczesniej zwalnial slot po TTL jak niewypelniony LIMIT.
#
# Co nowego w V6.76 (26.10 - raport badawczy Elliotta i chronologiczne A/B; BEZ ZMIAN handlu):
#  - elliott_stats.csv: rozliczone wyniki grup z podzialem na typ/strone/rodzaj wejscia; BRAK != NIEOBLICZONE.
#  - elliott_portfolio_ab.csv: deterministyczna symulacja porownawcza wyboru sygnalow na zdjeciach
#    przy 3 aktywnych pozycjach/kierunek, bez zajmowania kapitalu niewypelnionymi LIMIT;
#    uproszczenie badawcze, NIE jest oficjalnym wynikiem portfela ani backtestem live-ryzyka.
#  - Elliott nadal NIE zmienia entry, SL, TP, oceny A-D, bramki, PROBA ani do_handlu.
#  - Poprawki: raporty liczone po split_stocks (tylko krypto, jak bramka) i tylko na setupach po filtrach
#    (elliott_tylko_po_filtrach) - wczesniej wchodzily akcje, setupy odfiltrowane i typy wylaczone.
#
# Co nowego w V6.75 (26.10 - Elliott: liczby do pozniejszego testu; BEZ ZMIAN handlu):
#  - elliott_vs_setup (tylko wiersze setupow): BRAK / ZGODNY_Z_ROZWIJAJACYM / PRZECIW_ROZWIJAJACEMU /
#    ZGODNY_Z_ZAKONCZONYM / PO_IMPULSIE (setup przeciw wlasnie potwierdzonemu impulsowi = gra na korekte).
#  - elliott_fala2_proc / elliott_fala4_proc (cofniecie w % fali 1 / 3), elliott_fala3_do_1 / elliott_fala5_do_1
#    (krotnosc fali 1), elliott_do_uniewaznienia_atr (zamkniecie 1h vs poziom uniewaznienia, ATR 1h; + = po stronie
#    impulsu). Tylko gdy faza rozpoznana. test_elliott_structure: 15 grup przypadkow.
#
# Co nowego w V6.74 (26.10 - doprecyzowanie klasyfikacji fal; BEZ ZMIAN handlu):
#  - ATR historyczny przypisany do chwili potwierdzenia pivotu, a nie do konca aktualnego okna.
#  - IMPULS_ROZWIJA_SIE (hipoteza) vs IMPULS_5_POTWIERDZONY (po potwierdzeniu piatego ekstremum).
#  - Niezalezny trend swingowy 4h i zgodnosc 1h/4h; nadal tylko diagnostyka.
#  - Test odpornosci dawnych pivotow na pozniejsza zmiane ATR.
#  - Poprawka: VERSION = 'V6.136' (bylo 'V6.73').
#
# Co nowego w V6.73 (26.10 - eksperymentalna klasyfikacja struktury Elliott; BEZ ZMIAN handlu):
#  - ELLIOTT_STRUCTURE.v1: potwierdzone pivoty 1h/4h (bez repaintingu), diagnostyka podobienstwa do zakonczonego impulsu pieciofalowego,
#    stan NIEJEDNOZNACZNE gdy reguly nie daja wiarygodnego rozpoznania. Wyłącznie nowe kolumny CSV.
#  - Bez zmiany ENTRY, SL, TP, ocen, PROBA i bramki; analiza na świecach dostępnych w chwili zdjęcia.
#  - test_elliott_structure() sprawdza pivoty, brak zaglądania w przyszłość, przypadek bez trendu oraz (poprawki)
#    impuls LONG i SHORT, odrzucenie gdy fala 3 najkrotsza i gdy fala 4 wchodzi na fale 1 (7 przypadkow).
#  - Poprawki: VERSION = 'V6.73' (bylo 'V6.72'); pivoty liczone wektorowo - petla po swiecach podwajala czas testu
#    wstecznego 7 dni (65 s -> 135 s w replay); docstring: ABC nie jest rozpoznawane.
#
# Co nowego w V6.72 (26.10 - bramka z waga zdjecia rynku; setupy BEZ ZMIAN):
#  - GATE_WAGA_ZDJECIA = True: progi bramki (GATE_MIN_AVG_R, GATE_MIN_EDGE_R), hipotezy i proby (po_reg_R / po_reg_edge)
#    licza sredni R i przewage jako srednia wynikow zdjec rynku (kazde zdjecie waga 1), a nie po transakcjach. t juz
#    bylo liczone po zdjeciach. Powod (test portfelowy 26.10 na archiwum 11-26.10, reguly V6.71): wszystkie sygnaly
#    +0.125R/transakcje, portfel 3 pozycje/kierunek ~0.00R (200 losowan), srednia po zdjeciach +0.04R - zysk siedzial
#    w kilku zdjeciach z 10-30 skorelowanymi sygnalami (+0.35R), ktorych portfel moze wziac tylko 3.
#  - gate.csv: sredni_R / sredni_edge_R = wartosci do progow (z waga zdjecia), + sredni_R_transakcje /
#    sredni_edge_R_transakcje (stare). stats(): + sredni_R_zdj / sredni_edge_R_zdj. test_waga_zdjec() przy starcie.
#
# Co nowego w V6.71 (26.10 - zmiany setupow po analizie tygodnia; kryterium jak dotad: obie polowy testu 30 dni):
#  - PULLBACK LONG wylaczony (WYLACZONE_TYPY): 30 dni LIMIT -0.10/-0.31R, MARKET -0.20/-0.21R; na zywo tygodnie
#    11-18.10 i 19-26.10: LIMIT -0.34/-0.01R, MARKET -0.39/-0.06R. Jego straty wlaczaly tez tryb ochronny LONG.
#  - CONT_SWEEP LONG LIMIT wylaczony (nowe WYLACZONE_WEJSCIA = typ+kierunek+wejscie): 30 dni -0.18/-0.22R,
#    tygodnie -0.36/0.00R. CONT_SWEEP LONG MARKET zostaje (30 dni +0.44R, 21 tr. - mala proba).
#  - CONT_SL_ATR 0.8 -> 1.2 (zapasowy SL CONT_SWEEP bez struktury 15m): wariant cont_sl_1.2 lepszy w obu polowach
#    30 dni (lacznie +4.4R / +3.4R; CONT_SWEEP SHORT +0.9R / +3.4R). Zmiana reguly -> REGULY_OD_STRONA: statystyka
#    bramki i proby CONT_SWEEP (v4 i v4.M, obie strony) liczona od 2026-10-26T08:00 UTC. Wariant kontrolny cont_sl_0.8.
#  - Wylaczone typy dalej liczone w tescie jako 'typ_wylaczony' (mozna przywrocic). Tryb ochronny bez wylaczonych
#    typow i wejsc (typ_wylaczony()).
#
# Co nowego w V6.70 (26.10 - stany PROBA; reguly setupow BEZ ZMIAN):
#  - proba_stan przy kandydatach do proby: NOWA / KONTYNUACJA ZLECENIA / POZYCJA JUZ OTWARTA /
#    ZABLOKOWANA: INNA PROBA NA COINIE / ZABLOKOWANA: LIMIT PROB / ZABLOKOWANA: DRUGI SETUP W RAPORCIE
#    (proba_kontynuacja zostaje - mowi tylko, czy setup zajmuje miejsce w limicie).
#  - Ten sam event_id z wpisem 'otwarty' blokuje zawsze, takze gdy w dzienniku jest tez wpis 'oczekuje' (dziennik
#    ma deduplikacje po event_id, wiec to zabezpieczenie na wypadek niespojnych danych). test_proba_limit: 13 przypadkow.
#
# Co nowego w V6.69 (25.10 - PROBA i opis OI; reguly setupow BEZ ZMIAN):
#  - Kontynuacja proby 'do handlu' tylko, gdy to samo zlecenie LIMIT w dzienniku ma status 'oczekuje'. Gdy jest juz
#    wykonane ('otwarty'), setup jest nie do handlu: 'POZYCJA Z TEJ PROBY JUZ OTWARTA - nie otwieraj ponownie (...)'
#    (V6.67 pokazywal wtedy do_handlu=True - mozliwe drugie wejscie w ta sama pozycje; zauwazone w recenzji 25.10).
#    proba_wyniki: 'kontynuacje' licza tylko te do handlu. test_proba_limit: +1 przypadek (11).
#  - setups_all / ranking / setups*: oi_zrodlo, oi_override_powod i oi_niepotwierdzone (True = coin dopuszczony,
#    bo OI WEEX < progu bez potwierdzenia z Gate) - do pozniejszego porownania wynikow tych setupow.
#
# Co nowego w V6.68 (25.10 - prog OI tylko z potwierdzeniem Gate; reguly setupow BEZ ZMIAN):
#  - OI_PROG_TYLKO_Z_GATE = True: 'OI < 1.0M' blokuje tylko, gdy OI na Gate tez jest ponizej progu. OI WEEX bez danych
#    Gate nie blokuje (jak OI niewiarygodne). Podstawa (archiwum 11-25.10, 16 392 rozliczone): coiny blokowane tylko
#    przez OI +0.022R (n 1097, 16 coinow) = tradowalne +0.022R (n 8812); po filtrach -0.042R vs -0.055R. OI WEEX dla
#    tych coinow bywa absurdalne (1000XEC 568 USDT przy arkuszu 1.3M). Filtr arkusz/wolumen/spread BEZ ZMIAN
#    (te coiny -0.042R, t -1.75, poslizg 12.4 bps). Test i bramka i tak licza wszystkie coiny - zmiana dotyczy tylko
#    tego, ktore biezace setupy sa 'do handlu'. oi_zrodlo / oi_override_powod opisuja decyzje.
#
# Co nowego w V6.67 (25.10 - poprawka limitu PROBA; reguly setupow BEZ ZMIAN):
#  - Kontynuacja proby tylko dla TEGO SAMEGO zlecenia: ten sam event_id co w dzienniku, LIMIT i to samo wejscie.
#    Wczesniej wystarczal ten sam coin i kierunek: inny setup (np. AGT ODBICIE 25.10 15:00, wejscie 0.05972, SL 0.04993,
#    przy aktywnej probie AGT z 24.10: wejscie 0.07406, SL 0.05641) byl 'do handlu' bez miejsca w limicie prob,
#    bez wpisu do dziennika i bez ekspozycji -> mozliwa druga pozycja na tym samym coinie niewidoczna dla limitow.
#    Teraz: nie do handlu, w 'warunki' opis aktywnej proby z dziennika (typ, wejscie, SL, od kiedy, status).
#  - test_proba_limit: +4 przypadki (ten sam LIMIT, inna okazja, MARKET po innej cenie, LIMIT po innej cenie).
#  - _proba_rozlicz: kolumny tekstowe dziennika (status, fill_utc, exit_utc, niejednoznaczne) jako object przed wpisem
#    (pusta kolumna wczytana jako float64 - pandas 3: TypeError przerywal skrypt, pandas 2.x: ostrzezenie).
#
# Co nowego w V6.66 (25.10 - zapis plikow i autotest; reguly setupow BEZ ZMIAN):
#  - dopisz_log(): gdy nie da sie przeniesc niedokonczonego wiersza do pliku diagnostycznego, dziennik zostaje bez zmian
#    i nic nie jest do niego dopisywane w tym uruchomieniu (wczesniej: dopisanie od nowej linii obok uszkodzonego
#    wiersza). Reszta kolektora dziala dalej; dane z uruchomienia sa w zipie. Zwraca True/False.
#  - movers.csv (MOVERS_COLS) i summary.csv (SUMMARY_FIRST): naglowek takze przy pustej liscie.
#  - test_dziennikow() przy starcie (12 przypadkow): zapis/dopisanie, przerwany rekord, przerwany w polu, przerwany
#    naglowek, kolejnosc/mniej kolumn, nowa kolumna, nowa linia w polu, blad diagnostyki, pusty df, puste CSV z 'first'
#    i 'cols'. Blad testu przerywa skrypt (jak pozostale testy startowe).
#
# Co nowego w V6.65 (25.10 - tylko zapis plikow; reguly setupow BEZ ZMIAN):
#  - write_csv(): pusty plik zawsze z naglowkiem. setups.csv / setups_market.csv / setups_limit.csv dostaja pelny
#    zestaw kolumn z setups_all.csv, ranking_market.csv / ranking_limit.csv z ranking.csv, reszta kolumny z 'first'.
#    Wczesniej przy braku setupow plik byl calkiem pusty i pd.read_csv konczyl sie bledem EmptyDataError.
#
# Co nowego w V6.64 (25.10 - dzienniki: niedokonczony wiersz po przerwanym zapisie; reguly setupow BEZ ZMIAN):
#  - odetnij_niedokonczony_wiersz(): gdy dziennik nie konczy sie nowa linia (przerwany zapis), fragment za ostatnia
#    nowa linia trafia do <plik>.uszkodzone.txt (z czasem i zakresem bajtow), jest sprawdzany i dopiero wtedy
#    odcinany. V6.63 tylko przenosil nowe wiersze do nowej linii - a pd.read_csv(on_bad_lines='skip') wczytywal
#    uciety wiersz z mniejsza liczba pol jako rekord z pustymi polami. Blad zapisu diagnostyki -> bez obcinania.
#  - dopisz_log(): nowe linie w polach tekstowych -> spacje, zeby 1 linia pliku = 1 rekord (warunek wykrywania).
#  - Zapis przerwany jeszcze w naglowku: caly plik to fragment, dziennik zaczyna sie od nowa z naglowkiem.
#
# Co nowego w V6.63 (25.10 - dzienniki na Drive i opis OI; reguly setupow BEZ ZMIAN):
#  - dopisz_log(): wspolne dopisywanie do dziennikow na Drive (crowd_history, gate_stats_history, squeeze_log,
#    ksiazka_log). Gdy poprzedni zapis zostal przerwany (plik nie konczy sie nowa linia), ucieta linia zostaje
#    osobno i nie skleja sie z nowym wierszem (przy analizie: pd.read_csv(..., on_bad_lines='skip')). Gdy zmienia sie
#    zestaw kolumn: nowe kolumny -> plik przepisany raz atomowo (z kopia .bak) z suma kolumn; brakujace kolumny ->
#    puste pola. Wczesniej mode='a' dopisywal wiersze pod stary naglowek (kolumny przesuniete).
#  - ksiazka_log: okazja_id i event_id (jak w archiwum testu) + setup_type - ten sam setup z kolejnych uruchomien
#    to ta sama okazja; test scian po tygodniu liczy okazje, nie zdjecia.
#  - summary.csv: oi_zrodlo (WEEX / Gate / brak), oi_override_powod (gdy prog OI liczony z Gate - liczby WEEX i Gate)
#    oraz gate_wiek_h (wiek ostatniej zamknietej godziny Gate; starsze niz 3 h i tak nie sa uzywane).
#
# Co nowego w V6.62 (25.10 - przejrzystosc listy kontraktow i filtra OI; reguly setupow BEZ ZMIAN):
#  - Prog OI (MIN_OI_USDT) po porownaniu z Gate: gdy OI WEEX jest 'niezgodne z Gate', a OI na Gate >= progu,
#    blokada 'OI < 1.0M' jest zdejmowana (25.10: BTW przy wolumenie 133M USDT i OI Gate 31M odpadal, bo WEEX
#    pokazal OI 0.01x Gate; tak samo IRYS, UAI). Tylko w te strone: male OI na Gate NIE blokuje (inna gielda,
#    plynnosc WEEX mierza wolumen, spread i arkusz). Kolumna oi_prog_z_gate w summary.csv.
#  - universe.csv: kazdy kontrakt z exchangeInfo z rankiem wolumenu, zmiana 24h i powodem pominiecia
#    (wolumen < MIN_QUOTE_VOL_USDT, poza TOP_N, nie USDT perpetual, brak tickera). ALWAYS_INCLUDE: komunikat,
#    gdy symbolu z listy nie ma na WEEX (wczesniej pomijany bez sladu).
#  - movers.csv: top 15 wzrostow i spadkow 24h z powodem 'nie do handlu', setupami (z filtrami) i powodami
#    odrzucenia przy budowie (odrzucone.csv) - odpowiedz na 'dlaczego nie ma setupu na X'.
#  - ODBICIE: kolumny odbicie_pierwsza_cena (zamkniecie pierwszej swiecy 5m z ostatnich ODBICIE_KLUCZ_H h ze
#    spadkiem >= ODBICIE_SPADEK_PCT) i odbicie_gonienie_atr ((cena - pierwsza) / ATR 1h). Strefa ODBICIE to
#    biezaca cena, wiec przy kolejnych uruchomieniach wejscie idzie w gore (25.10 AGT: 0.06654 -> 0.06881).
#    TYLKO INFORMACJA + zapis w tescie wstecznym - filtr dopiero po sprawdzeniu w archiwum.
#
# Co nowego w V6.61 (24.10 - arkusz zlecen WEEX przy setupach; TYLKO INFORMACJA + dziennik do testu):
#  - ksiazka_setup(): z arkusza zlecen WEEX (200 poziomow, pobierany juz wczesniej) dla kazdego setupu:
#    ks_imb_1pct / ks_imb_2pct (przewaga kupujacych + / sprzedajacych -), ks_opor_x + ks_opor_cena (najwieksza sciana
#    PRZECIW na drodze do TP1, jako krotnosc mediany poziomu), ks_wsparcie_x + ks_wsparcie_cena (najwieksza sciana PO
#    NASZEJ stronie miedzy SL a wejsciem), ks_zasieg_tp (czy arkusz siega TP1), ks_opor_obrot / ks_wsparcie_obrot
#    (sciana jako czesc obrotu 24h). Tylko w setups_all.csv i dzienniku - NIE w raporcie: arkusz WEEX jest w duzej
#    czesci drabinka market makerow (np. PUMP: 'sciana' 17.9 mln USDT przy obrocie 1.35 mln USDT/24h), wiec bez testu
#    takie sciany wprowadzalyby w blad.
#  - Arkusz to stan z jednej chwili (sciany bywaja przestawiane) - NIE zmienia oceny ani dopuszczenia.
#    Historii arkusza nie ma, wiec cechy sa zapisywane na Drive (KSIAZKA_LOG) przy kazdym uruchomieniu -
#    po ~tygodniu test: czy setupy ze sciana przed TP / wsparciem przed SL konczyly sie inaczej.
#  - Ilosc ponizej minimum kontraktu, ale ryzyko min. ilosci <= 1.5x zalozonego: 'ilosc' = minimum (bylo 0.0).
#
# Co nowego w V6.60 (24.10 - ODBICIE od razu jako PROBA):
#  - PROBA_Z_BADANIA = ('ODBICIE',): ODBICIE.v2 moze wejsc do PROBY (ryzyko x0.5 = ~0.75% kapitalu, max 2 PROBY naraz)
#    bez czekania na 20 okazji na zywo. Podstawa: badanie 39 dni / 229 coinow +0.47R (polowy +0.34/+0.61, t 3.1),
#    stres-test poslizgu +200 bps nadal dodatni. Bezpiecznik: od 10 rozliczonych okazji na zywo sredni wynik < 0
#    -> z powrotem OBSERWACJA. Od 20 okazji zwykle reguly PROBA/bramki.
#  - OCHRONA_BEZ_TYPOW = ('ODBICIE',): tryb ochronny LONG (liczony z wynikow PULLBACK/CONT_SWEEP/REV_MSS) nie blokuje
#    ODBICIA i nie zmniejsza jego ryzyka - sygnaly ODBICIA powstaja wlasnie w spadkach, gdy ochrona LONG jest wlaczona.
#    Dane mieszane (archiwum 7 dni: +0.04R przy ochronie, test 30 dni: +1.49R) - dlatego tylko z ryzykiem PROBA.
#    Alarm BTC, dzienny limit straty, limit ekspozycji i minimum kontraktu dzialaja dalej.
#  - Dzwignia w raporcie jak dotad: 'bezp. max' tak, by likwidacja byla wyraznie pod SL (ODBICIE ma SL 11-23%).
#
# Co nowego w V6.59 (24.10 - bezpieczny zapis historii na Drive, bez zmian w sygnalach):
#  - BLAD: przy bledzie odczytu archiwum (backtest_archive_v63.csv) skrypt pisal 'zaczynam od nowa' i NADPISYWAL
#    archiwum samymi nowymi wierszami -> utrata calej historii (bramka, tryb ochronny, obserwacja ODBICIE od zera).
#    Teraz: proba odczytu kopii .bak; gdy i ona zawiedzie -> archiwum NIE jest zapisywane, handel zablokowany
#    w tym uruchomieniu ('archiwum: blad odczytu'), jak przy braku Drive.
#  - Zapis atomowy (plik tymczasowy + zamiana) z kopia poprzedniej wersji (.bak): archiwum, dziennik PROBA,
#    reguly_od.json. Przerwany zapis (rozlaczenie Colab) nie zostawia uszkodzonego pliku.
#  - test_zapisu() przy starcie (zapis atomowy, odczyt z .bak, blokada zapisu przy bledzie odczytu).
#
# Co nowego w V6.58 (24.10 - poprawki symulacji i statystyki, bez nowych strategii):
#  - MARKET: gdy otwarcie pierwszej swiecy po zdjeciu jest juz za SL albo za TP1 -> 'anulowany' (nie wchodzimy).
#    Wczesniej: za SL liczone -1R (sztuczna strata), za TP1 liczone jako zysk abs(TP - wejscie) (sztuczny zysk -
#    np. short BNC 14.10 otwarty 4.4955 ponizej TP 5.0135 dostal +0.745R). Archiwum 24.10: za TP 2/2925 (oba PULLBACK
#    SHORT - wylaczony), za SL 0. Wplyw na wyniki znikomy, ale wynik nie moze pochodzic z wejscia, ktorego by nie bylo.
#  - REGULY_OD_STRONA: osobna data startu statystyki dla typ_wersja x kierunek. CONT_SWEEP.v4 LONG liczony od
#    24.10 08:00 (zmiana wejscia z V6.57), CONT_SWEEP.v4 SHORT bez zmian. Bramka/PROBA nie mieszaja juz starego
#    i nowego wejscia LONG (wczesniej 317 okazji ze starym wejsciem, -0.03R).
#  - test_market_luka() przy starcie.
#
# Co nowego w V6.57 (24.10 - CONT_SWEEP LONG: LIMIT na samym poziomie):
#  - CONT_ENTRY_FRONT_ATR 0.05 -> 0.0 (LONG). Wariant cont_long_0.00 lepszy w obu polowach 30 dni w kazdym pomiarze:
#    23.10 07:28 +11.2/+7.7R, 23.10 18:13 +12.0/+8.8R, 23.10 20:28 +12.0/+7.3R, 24.10 06:32 +11.7/+8.7R.
#    CONT_SWEEP LONG: -0.25R -> -0.15R na transakcje (odpadaja niewypelnione retesty, glownie stratne). Typ dalej
#    ujemny - bramka dalej go nie dopuszcza. Wersja typu bez zmian (v4), zeby nie zerowac statystyki CONT_SWEEP SHORT.
#  - Warianty: cont_long_0.00 usuniety (teraz domyslne), dodany cont_long_0.05 (stare ustawienie, do kontroli).
#  - Warianty 24.10: odbicie_po_pompie gorszy w obu polowach (-1.4 / -1.7R) - wylaczenie filtra w V6.56 potwierdzone.
#
# Co nowego w V6.56 (24.10 - ODBICIE.v2: prog 10%, TP 8 ATR, 48h, bez filtra po_pompie):
#  - Badanie 24.10 (229 coinow, 1h, 15.09-23.10, koszty): wyjscia ODBICIE (SL 2 ATR):
#      prog 12%, TP 4 ATR / 24h:  +0.19R (polowy +0.11 / +0.27), 3.7 sygn./dzien
#      prog 10%, TP 4 ATR / 24h:  +0.30R (+0.18 / +0.41), 30/39 dni na plus, t(dni) 3.9, 6.1 sygn./dzien
#      prog 10%, TP 8 ATR / 48h:  +0.47R (+0.34 / +0.61), t(dni) 3.1  <- wybrane (duze ruchy jak BIRB 24.10)
#      prog 8%,  TP 8 ATR / 48h:  +0.42R (+0.26 / +0.58), 10.7 sygn./dzien
#    Wszystkie progi i wyjscia dodatnie w obu polowach (stabilny obszar, nie pojedynczy szczyt). Przy dluzszym
#    trzymaniu sygnaly 'po pompie' przestaja byc gorsze (TP8/48h: +0.23R) -> filtr po_pompie WYLACZONY
#    (ODBICIE_PO_POMPIE_PCT = None; kolumna wzrost_przed_24h_pct zostaje). 24.10 BIRB (odfiltrowany) +150%.
#  - Nowa wersja ODBICIE.v2 = nowa statystyka bramki (od pierwszego uruchomienia V6.56). Warianty testu 30 dni:
#    odbicie_v1 (12%, TP4, 24h) i odbicie_po_pompie (filtr 30%) do porownania.
#
# Co nowego w V6.55 (24.10 - BLAD daty 'od kiedy reguly' dla nowych typow):
#  - BLAD: reguly_od = najwczesniejsze run_utc w archiwum, ale archiwum przy kazdym uruchomieniu nadpisuje run_utc
#    setupow z ostatnich 7 dni (test wsteczny liczy je od nowa). Dla nowego typu (ODBICIE) data przesuwala sie
#    na KAZDE uruchomienie -> licznik okazji po ustaleniu regul stal na 0 (PROBA nie mogla sie pojawic przez ok. 2 tyg.).
#  - Teraz pierwsza data kazdej wersji typu jest zapamietywana na Drive (reguly_od.json, tylko minimum) i nie cofa sie.
#    Zasiew: ODBICIE.v1 / .v1.M od 2026-10-23T18:13 (pierwsze uruchomienie V6.53; filtr po_pompie z V6.54 tylko
#    odrzuca setupy, a bramka liczy setupy po filtrach).
#  - test_reguly_od() przy starcie
#
# Co nowego w V6.54 (23.10 - filtr 'po_pompie' dla ODBICIE; reszta bez zmian):
#  - ODBICIE z filtrem 'po_pompie', gdy coin wzrosl o > ODBICIE_PO_POMPIE_PCT (30%) w 24h PRZED spadkiem (zamkniecie 1h
#    sprzed 4h vs sprzed 28h). Badanie 23.10 (141 sygnalow, 6 tyg.): po pompie -0.06R (polowy -0.14 / +0.02),
#    reszta ok. +0.29R. Odfiltrowane sygnaly dalej sa w archiwum (kolumna filtr) - do oceny na nowych danych.
#
# Co nowego w V6.53 (23.10 - NOWY TYP ODBICIE, tylko obserwacja/proba; reszta bez zmian):
#  - ODBICIE LONG: coin spadl o >= ODBICIE_SPADEK_PCT (12%) w 4h -> wejscie PO RYNKU, SL 2 ATR 1h, TP 4 ATR 1h,
#    max 24h, BEZ zamykania 50% na +1R (test 23.10: czesc na 1R obniza wynik z +0.17R do +0.10R).
#  - Badanie 23.10 (229 coinow, swiece 1h 14.09-23.10, z kosztami): +0.17R/transakcje (138), obie polowy +0.17R,
#    zysk w 23 z 37 dni; losowy LONG w tym okresie -0.12R. Sprawdzone i odrzucone tego samego dnia: LONG po wybiciu
#    z kompresji (-0.3..-0.6R), LONG po duzym wzroscie (~0R), SHORT po duzym wzroscie (-0.16..-0.40R).
#  - Ocena ODBICIE stala B (ocena punktowa A-D nie przewiduje wynikow - badanie 22.10). Bez filtra przeciw_trendowi,
#    anomalii wolumenu, rezimu 1h i BTC 1h przeciw (typ z zalozenia gra przeciw ruchowi). Max ryzyko 25% ceny
#    (2 ATR po spadku to zwykle 10-15%) - pozycja odpowiednio mniejsza.
#  - Bramka liczy ODBICIE od pierwszego uruchomienia V6.53 (reguly_od) -> pierwsze PROBA najwczesniej po 20
#    rozliczonych okazjach na NOWYCH danych (ok. tydzien). Do tego czasu OBSERWACJA.
#  - Wariant testu 30 dni 'odbicie_10' (prog 10%). Zmiana SETUP_TYPES -> pierwsze uruchomienie liczy test 30 dni od nowa.
#
# Co nowego w V6.52 (23.10 - PROBA tylko dla typow z nieujemnym wynikiem netto):
#  - PROBA_MIN_N_WYNIK 50 -> 20 (= PROBA_MIN_OKAZJE): proba wymaga sredniego wyniku netto po ustaleniu regul >= 0
#    od pierwszej dopuszczalnej okazji, nie dopiero od 50. 23.10: CONT_SWEEP.v4.M LONG mial 41 okazji, sr. -0.24R,
#    ale przewage nad losowym wejsciem > 0 (losowe traci jeszcze bardziej) -> 2 proby (ACE, PHA) = 2 x SL (-2.2R).
#    Przewaga nad losowym nie chroni przed typem, ktory sam traci pieniadze.
#  - Sprawdzone 23.10 i NIE wdrozone (brak stalego wyniku w 2 zrodlach i 2 polowach): filtr waskiego przedzialu BTC
#    (zakres 12h w ATR), efektywnosc ruchu BTC 12h, liczba retestow poziomu CONT_SWEEP.
#
# Co nowego w V6.51 (23.10 - krotszy czas pracy; strategia BEZ zmian):
#  - Test historyczny 30 dni (ok. 9 min z wariantami) liczony od nowa tylko w oknie HIST_OKNO_UTC (domyslnie 02-06 UTC,
#    pierwszy poranny przebieg), raz na dobe. Poza oknem z cache; awaryjnie od nowa, gdy cache starszy niz
#    HIST_REFRESH_MAX_H (36 h). Wczesniej: co 24 h od ostatniego liczenia, czyli o przypadkowej porze w ciagu dnia.
#  - Usuniete warianty fvg_0.75 (gorzej w obu polowach: -17.0R / -3.7R) i market_zone_0.10 (sredni R gorszy niz baza,
#    poprawa sumy tylko przez mniej transakcji). Mniej wariantow = ok. 3 min krocej przy liczeniu od nowa.
#  - Usuniecie wariantow NIE wymusza przeliczenia: gdy nowa lista jest podzbiorem policzonej, wyniki brane z cache.
#  - test_hist_okno() przy starcie
#
# Co nowego w V6.50 (23.10 - nowe WARIANTY WEJSC w tescie 30 dni; strategia BEZ zmian):
#  - HIST_WARIANTY: + fvg_0.75 (LIMIT glebiej w FVG), market_zone_0.15 / market_zone_0.10 (MARKET blizej strefy),
#    cont_long_0.00 (LIMIT CONT_SWEEP LONG dokladnie na poziomie). Wynik w hist_warianty.csv (obie polowy 30 dni).
#  - usuniety sl_bufor_0.25 (gorszy w obu polowach: -19.0R / -9.6R). cont_sl_1.2 zostaje (22.10 pierwszy raz lepszy
#    w obu polowach, +6.6R / +3.5R, ok. +0.007R na transakcje - czekamy na potwierdzenie).
#  - Nie testowane ponownie (odrzucone 14.10): FVG CE 0.5, CONT_SHORT 0.05, CONT_LONG 0.15.
#  - Pierwsze uruchomienie przelicza baze + warianty (dluzej o ok. 10-15 min), potem z cache co HIST_REFRESH_H.
#
# Co nowego w V6.49 (22.10 - dziennik PROBA odporny na bledy odczytu/zapisu; strategia bez zmian):
#  - BLAD: przy bledzie odczytu proba_log.csv dziennik byl traktowany jak pusty, a na koncu ZAPISYWANY ponownie
#    (z samymi nowymi wpisami) -> utrata historii prob i limit liczony od zera. Teraz: blad odczytu = brak zapisu,
#    wszystkie proby w tym raporcie nie do handlu ('dziennik PROBA: blad odczytu').
#  - Blad zapisu dziennika: NOWE proby nie do handlu ('dziennik PROBA: blad zapisu'), kontynuacje bez zmian.
#  - Uwaga: DO GRY juz liczy proby jako zajete miejsca w limicie kierunku (wybor_ryzyka) - osobny wspolny limit
#    niepotrzebny. Ekspozycja w USDT z pozycji pominieta (pozycje podawane recznie, poza skryptem).
#  - test_proba_dziennik(), test_ekspozycji_konfig() i przypadek 6 w test_ekspozycji (SL przesuniety czesciowo,
#    LONG i SHORT naraz)
#
# Co nowego w V6.48 (22.10 - R:R >= 2.5 bez punktu w ocenie; reszta bez zmian):
#  - Badanie 22.10, archiwum WEEX 11-22.10 (2311 okazji): R:R >= 2.5 gorszy w 3/3 okresach (SL 52% vs 38% przy 2-2.5),
#    w tescie 30 dni (symulacja) raz lepszy, raz gorszy -> brak dowodu na +1. OCENA_RR_PKT 1 -> 0.
#  - Sprawdzona i ODRZUCONA ocena z wynikow (oczekiwany R z ostatnich 10 dni): w walk-forward na archiwum WEEX
#    (15-22.10) A/B wypadaly gorzej niz C/D we wszystkich 5 wariantach - nie wdrozona.
#
# Co nowego w V6.47 (21.10 - limit 3 pozycji z ryzykiem na kierunek, decyzja uzytkownika):
#  - MAX_RYZYKO_KIERUNEK_PCT 3.0 -> 4.5 (= 3 pozycje x 1.5%). Setupy, bramka, PROBA i ochrona bez zmian
#  - KAPITAL_USDT 190 -> 200 (stan konta 21.10 wieczor)
#
# Co nowego w V6.46 (21.10 - LIMIT EKSPOZYCJI egzekwowany dla DO GRY, bez zmian w setupach):
#  - 21.10 18:07: PULLBACK LONG przeszedl bramke -> 26 setupow 'DO GRY' z pelnym ryzykiem (16 A/B = 24% ryzyka
#    w jednym kierunku). Teraz 'DO GRY' zostaje tylko setup wybrany w limicie MAX_RYZYKO_KIERUNEK_PCT (w_limicie_ryzyka);
#    pozostale: do_handlu False, warunek 'limit ekspozycji (...)', status OBSERWACJA
#  - otwarte pozycje licza sie do limitu tylko, gdy maja ryzyko (brak SL albo SL gorszy niz wejscie);
#    pozycja z SL na wejsciu lub w zysku nie zajmuje miejsca. Pozycje z konta WEEX (klucz API) albo otwarte_pozycje.csv
#  - test_ekspozycji() przy starcie (5 przypadkow)
#
# Co nowego w V6.45 (21.10 - lista obserwowanych TradFi bez 4 ETF-ow o szerokim spreadzie, bez zmian w handlu):
#  - z WATCH_TRADFI usuniete AMZU, GGLL, AAPU, MSFU: 20-21.10 spread 40-66 bps takze w sesji USA (prog 10 bps),
#    wolumen < 0.5M/24h -> nigdy 'do handlu'. Gdyby weszly do top 60 TradFi wg wolumenu, nadal beda skanowane
#
# Co nowego w V6.44 (21.10 - ryzyko na transakcje 1.5%, decyzja uzytkownika):
#  - RYZYKO_NA_TRANSAKCJE_PCT 1.0 -> 1.5 (pelne ryzyko ~2.85 USDT, PROBA x0.5 ~1.4 USDT, z trybem ochronnym ~0.7 USDT)
#  - MAX_RYZYKO_KIERUNEK_PCT 2.0 -> 3.0 (ostrzezenie o lacznym ryzyku w jednym kierunku, proporcjonalnie)
#  - DZIENNY_LIMIT_R 3R bez zmian (= ok. -4.5% konta dziennie). Setupy, bramka, PROBA i ochrona bez zmian
#
# Co nowego w V6.43 (21.10 - FAIL CLOSED bez Dysku, bez zmian w setupach):
#  - brak Google Drive (na zywo) = zaden setup nie jest do handlu (ani DO GRY, ani PROBA): bez dziennika nie wiadomo,
#    ile prob jest aktywnych, a bramka liczy od zera. Replay dziala jak dotad (analiza, PROBA liczona bez dziennika)
#  - ostrzezenie, gdy w dzienniku jest wiecej aktywnych prob niz PROBA_MAX_NA_RAZ (nowe i tak nie sa wystawiane)
#  - raport i run_meta: liczba kontraktow osobno krypto i TradFi (symbols_krypto / symbols_tradfi)
#  - pula bez zmian: TOP_N 170 krypto + TRADFI_N 60 (+ lista obserwowanych)
#
# Co nowego w V6.42 (21.10 - ostrzezenie o braku Google Drive, bez zmian w handlu):
#  - bez Dysku (poza replay): duzy komunikat na poczatku i na koncu, w raporcie, plik BRAK_DYSKU.txt w zipie
#    i nazwa zipa z dopiskiem _BEZ_DYSKU. Bez Dysku nie ma archiwum, bramki, dziennika PROBA ani historii OI
#    (bramka liczy od zera -> nic nie przechodzi do gry; tryb ochronny i hipoteza z jednego przebiegu = niewiarygodne)
#
# Co nowego w V6.41 (poprawki limitu PROBA i korelacji BTC, bez zmian setupow):
#  - otwarte pozycje PROBA licza sie do limitu tak dlugo, jak pozostaja otwarte; 48h ogranicza tylko oczekujace
#  - corr_btc=0 nie jest juz zamieniane na 1 w filtrze trendu BTC
#  - testy: test_proba_wiek() (otwarta >48h zajmuje miejsce, stara oczekujaca nie) + 3 przypadki korelacji w test_bramki
#  - (21.10, poprawki do wersji uzytkownika: VERSION = 'V6.41', testy korelacji dopisane)
#
# Co nowego w V6.40 (21.10 - PROBA: wymagany nieujemny wynik NETTO, bez zmian w setupach):
#  - od PROBA_MIN_N_WYNIK (50) okazji po ustaleniu regul proba wymaga sredniego wyniku netto >= 0 (po_reg_R), nie tylko
#    przewagi nad losowym wejsciem. 21.10: PULLBACK LONG mial 391 okazji, -0.11R netto, ale przewage +0.12R (t 1.0)
#    -> wchodzil do proby (BAT/RAY -1.06R). Brak danych wyniku przy >= 50 okazjach = brak proby
#  - test_bramki: +4 przypadki (x3 tryby)
#
# Co nowego w V6.39 (21.10 - limit PROBA liczy tez proby z dziennika, bez zmian w setupach):
#  - PROBA_MAX_NA_RAZ (2) obejmuje teraz: pozycje otwarte + zlecenia oczekujace (nie wygasle) z dziennika PROBA
#    + nowe proby z tego raportu. Wczesniej limit liczyl tylko biezacy raport (21.10 03:32: ENS/LINK czekaly,
#    a skrypt wystawil jeszcze ADA/XLM = 4 proby naraz)
#  - setup z biezacego raportu na tym samym coinie i kierunku co aktywna proba = KONTYNUACJA (status PROBA, nie
#    zajmuje nowego miejsca, nie dopisywany drugi raz do dziennika)
#  - dziennik rozliczany PRZED wyborem nowych prob (wygasle zlecenia zwalniaja miejsce od razu)
#  - test_proba_limit() przy starcie (6 przypadkow)
#
# Co nowego w V6.38 (21.10 - hipoteza: KAZDY rodzaj wejscia sprawdzany osobno, bez zmian w setupach/SL/TP/filtrach):
#  - LIMIT i MARKET osobno musza miec: progi bramki (>= HIP_MIN_N_WEJSCIE tr., sr >= +0.05R, edge >= 0.05, t >= 1),
#    >= HIP_MIN_ZDJEC zdjec rynku, >= HIP_MIN_OKAZJI unikalnych okazji i dodatni wynik PO FILTRACH (sr > 0)
#  - test_hipotezy() przy starcie (8 przypadkow): m.in. slaby MARKET nie przechodzi dzieki dobremu LIMIT (i odwrotnie),
#    za malo zdjec / okazji / ujemny wynik po filtrach w jednej grupie blokuje tylko te grupe
#
# Co nowego w V6.37 (20.10 - hipoteza: TYLKO OSTRZEJSZE warunki odblokowania, bez zmian w setupach):
#  - LIMIT i MARKET liczone osobno: setupy z hipotezy odblokowane tylko dla rodzaju wejscia, ktory sam spelnia progi
#    (>= HIP_MIN_N_WEJSCIE transakcji, sr >= +0.05R, edge >= 0.05, t >= 1) - oprocz wyniku lacznego
#  - niezaleznosc: wymagane >= HIP_MIN_ZDJEC zdjec rynku i >= HIP_MIN_OKAZJI unikalnych okazji (nie 50 transakcji z kilku chwil)
#  - wynik po rzeczywistych filtrach (filtr == '') raportowany osobno i musi byc dodatni (sr > 0) do odblokowania
#  - blad liczenia hipotezy zapisywany w hipotezy.csv (status 'BLAD: ...') i w run_meta, zamiast tylko w logu
#
# Co nowego w V6.36 (20.10 - HIPOTEZA ZAPISANA PRZED TESTEM, bez zmian w setupach):
#  - Badanie 20.10 (312 kombinacji typ x warunek, 3 okresy): jedyna spojna grupa dodatnia we wszystkich okresach to
#    CONT_SWEEP SHORT przy KOMPRESJI zmiennosci 1h (bb_rank_1h < 25 lub squeeze_1h): +0.08R / +0.10R / +0.17R
#    (n 76/93/173). Zastrzezenia: wynik skupiony w tyg. 42, wielokrotne testowanie -> to HIPOTEZA, nie przewaga
#  - Hipoteza liczona osobno OD HIPOTEZA_OD (tylko nowe dane, progi ustalone teraz i nie zmieniane): hipotezy.csv
#    (krypto, LIMIT+MARKET razem, archiwum po deduplikacji, bez warunku filtrow - tak jak w badaniu)
#  - Gdy hipoteza spelni progi bramki (>= 50 transakcji, sr >= +0.05R, edge >= 0.05, t >= 1): pasujace setupy dostaja
#    status_bramki 'DO GRY'. Ocena A-D, tryb ochronny, alarm BTC i pozostale blokady dzialaja jak dotad
#  - Kolumny setupow: hipoteza, hip_n, hip_R, hip_status
#
# Co nowego w V6.35 (20.10):
#  - WATCH_TRADFI: kontrakt z listy brany tylko, gdy WEEX oznacza go jako TradFi (underlyingType), nie tylko po nazwie
#    (krypto o tym samym tickerze nie wejdzie do puli akcji). W logu tylko liczba brakujacych, bez pelnej listy
#  - OI dla TradFi tylko informacyjnie: MIN_OI_TRADFI_USDT = 0. Zipy 19.10 (sesja USA): kontrakty blokowane WYLACZNIE
#    przez OI mialy dobra plynnosc (SPY: spread 0.13 bps, book 105M, OI 6.7k; EWY book 220M, OI 1.5k). O plynnosci
#    decyduja spread, glebokosc booka i wolumen 24h - bez zmian. Krypto: prog OI 1M bez zmian
#
# Co nowego w V6.34 (20.10):
#  - LISTA OBSERWOWANYCH TRADFI (WATCH_TRADFI): lewarowane ETF-y na akcje/indeksy (np. AMZU, TSLL, NVDL, MSTU, CONL,
#    TQQQ, SOXL) pobierane ZAWSZE, nawet spoza top 60 wg wolumenu. Brane tylko te, ktore istnieja na WEEX (lista w logu),
#    maks. WATCH_TRADFI_MAX wg wolumenu. Filtry plynnosci (spread, book, wolumen 500k, OI) i sesji USA BEZ zmian
#
# Co nowego w V6.33 (19.10):
#  - MIN_OI_TRADFI_USDT 100k -> 10k: OI WEEX jest niewiarygodne, a plynnosc TradFi i tak sprawdzaja spread
#    (MAX_SPREAD_BPS), glebokosc booka (MIN_DEPTH_USDT) i wolumen 24h. 19.10: 20 kontraktow blokowanych tylko przez OI
#    (np. SPY spread 0.1 bps, book 110M USDT przy OI 22k). Bez wplywu na test akcji (test nie uzywa tradable)
#
# Co nowego w V6.32 (19.10 - wielkosc pozycji, BEZ zmian w strategii):
#  - gdy ilosc z ryzyka po zaokragleniu do kroku kontraktu (quantityPrecision) wychodzi 0 albo ponizej minOrderSize:
#    ilosc_min (najmniejsza mozliwa), ryzyko_min_usdt (jej realna strata na SL), ilosc_za_mala=True + uwaga w planie
#  - 'do handlu' blokowane, gdy najmniejsza mozliwa pozycja ryzykuje wiecej niz ILOSC_MIN_MAX_RYZYKO x zalozone ryzyko
#    (powod w warunkach: 'ilosc ponizej minimum kontraktu ...'). test_bramki: +2 przypadki (x3 tryby)
#
# Co nowego w V6.31 (19.10 - kolejnosc blokad, BEZ zmian w strategii):
#  - badanie rynku oraz dodatkowe filtry wykonuja sie PRZED zapisem PROBA i wyliczeniem status_setupu
#  - dziennik PROBA, status oraz rozdzielone blokady odpowiadaja ostatecznym warunkom handlu
#
# Co nowego w V6.30 (19.10 - raport i dane, BEZ zmian w strategii):
#  - podsumowanie testu: 'TYP odrzucone' rozbite na LONG/SHORT (wczesniej mieszalo kierunki: 'PULLBACK odrzucone +0.13R'
#    to byly glownie wylaczone PULLBACK SHORT z okresu spadkow, a odrzucone PULLBACK LONG byly GORSZE od przepuszczonych:
#    archiwum -0.47R vs -0.39R, 30 dni -0.32R vs -0.02R - filtr 'koszt' dziala)
#  - kolumny blokady_bezpieczenstwa (tryb ochronny, alarm BTC, dzienny limit, nie tradable) i blokady_kwalifikacji
#    (filtry, ocena, rezim, BTC przeciw, bramka/proba) w setupach i rankingach
#  - qc.json: tylko problemy z danymi; informacje (zawyzone OI WEEX, roznica ticker/swieca ponizej PRICE_MISMATCH_PCT)
#    osobno w qc_info.json - nie jest bledem do naprawy (test 19.10: brak stalego mnoznika vs Gate.io)
#  - symbol odrzucony przez WEEX ('symbol is invalid', kod -1142, np. MSTUUSDT): po pierwszym bledzie swiec bez
#    dalszych zapytan i bez ponownej proby
#
# Co nowego w V6.29 (19.10 - nowe warianty testu, BEZ zmian w strategii):
#  - wyniki 19.10 (baza i warianty z tego samego okna, 30 dni do 12:00 UTC), suma R wariant - baza w polowach:
#    koszt_max_0.15: -4.1 / -40.2 (gorzej w obu) | azja_short_stary: +2.4 / -8.6 | sl_max_8pct: -5.7 / +7.3
#    -> wszystkie odrzucone, usuniete z HIST_WARIANTY
#  - nowe warianty SL: 'sl_bufor_0.25' (SETUP_SL_BUFFER_ATR 0.1 -> 0.25, SL dalej za swingiem),
#    'cont_sl_1.2' (CONT_SL_ATR 0.8 -> 1.2, zapasowy SL CONT_SWEEP bez struktury)
#
# Co nowego w V6.28 (19.10):
#  - V6.27 nie uniewaznial wariantow policzonych wczesniej zlym sposobem (cache z tym samym var_sig) -> klucz cache
#    wariantow ma teraz znacznik metody ('v627_to_samo_okno'): pierwsze uruchomienie przelicza baze + warianty razem
#
# Co nowego w V6.27 (19.10 - POPRAWKA testu wariantow, BEZ zmian w strategii):
#  - BLAD do V6.26: gdy zmienialy sie HIST_WARIANTY, a test bazowy byl w cache, warianty liczone byly na NOWYM oknie 30 dni,
#    a BAZA brana ze starego (np. sprzed 6 h) -> roznice wariant-baza mieszaly efekt ustawienia z przesunieciem okna
#    (widoczne 19.10: grupy LONG identyczne w wariantach 'azja_short_stary' i 'sl_max_8pct', a inne niz w BAZIE).
#    Teraz: przeliczenie wariantow = przeliczenie bazy na tych samych swiecach i tym samym oknie (raz, ~kilka minut)
#  - hist_warianty.csv: kolumna 'okno_utc' (chwila konca okna) - kontrola, ze baza i warianty sa z tego samego okna
#
# Co nowego w V6.26 (19.10 - tylko test A/B, BEZ zmian w strategii):
#  - HIST_WARIANTY + 'sl_max_8pct' (SETUP_MAX_RISK_PCT 8 zamiast 5): test 7 dni 19.10 - dodatkowe 149 tr. +0.05R / +0.01R
#    w polowach, po 15.10 +0.10R (98 tr.). Wynik 30 dni w hist_warianty.csv; zmiana domyslnej tylko gdy lepiej w OBU
#    polowach 30 dni. Odrzucone 19.10: SETUP_MAX_SL_ATR 4 (dodatkowe 44 tr. -0.16R / -0.24R)
#  - sprawdzone 19.10 i bez zmian: prog alarmu BTC (1.0 ATR lepszy tylko na poziomie transakcji, po zdjeciach brak
#    roznicy: p1 -0.003R, p2 +0.10R, t 0.84; blokowalby polowe czasu) - zostaje 1.5
#
# Co nowego w V6.25 (19.10 - odpornosc, BEZ zmian w strategii):
#  - bramka/proba: _num odrzuca NaN i +/-Infinity; licznik okazji musi byc skonczona liczba calkowita >= 0
#    (inaczej: 'nieprawidlowy licznik okazji' -> nie do proby)
#  - dziennik PROBA: kazdy rekord sprawdzany przed rozliczeniem (data zdjecia, strona, entry/SL/TP1 > 0, skonczone,
#    kolejnosc SL < entry < TP1 dla LONG i odwrotnie dla SHORT, wait_h). Zly rekord -> status 'blad_rekordu'
#    (+ powod), poza statystyka; blad jednego rekordu nie przerywa rozliczania pozostalych
#  - brak Google Drive przy trybie probnym: wyrazne ostrzezenie (dziennik tylko w zipie, nie bedzie rozliczany dalej)
#  - test_bramki: 20 przypadkow x 3 tryby = 60 (+ Infinity przewagi, nieprawidlowy licznik okazji);
#    test walidacji rekordow dziennika PROBA
#
# Co nowego w V6.24 (19.10 - tylko raport, BEZ zmian w strategii i bramce):
#  - OSTRZEZENIE O ROZBIEZNOSCI: strategia (typ x kierunek), ktora w calym tescie 30 dni ma wynik > 0, a po ustaleniu
#    regul (>= ROZB_MIN_N tr.) < 0 - albo w archiwum bez filtrow > 0, a po filtrach i po ustaleniu regul < 0.
#    hist_summary.csv: kolumna 'rozbieznosc'; run_meta: 'rozbieznosci'; raport: lista na gorze
#  - raport: gdy laczny wynik po ustaleniu regul < 0, a pojedyncze strategie wygladaja dodatnio - wyrazna linia,
#    ze dodatnie statystyki rodzin pochodza z proby projektowej
#
# Co nowego w V6.23 (19.10 - testy i przejrzystosc, BEZ zmian w strategii, Entry, SL, TP):
#  - test_bramki() sprawdza WSZYSTKIE tryby bramki (twardy / probny / etykieta) - 16 przypadkow x 3 tryby -
#    oraz zgodnosc status_setupu z do_handlu w kazdym przypadku. Blad przerywa skrypt
#  - apply_conditions czyta BRAMKA_TRYB bezposrednio (bez osobnej flagi BRAMKA_TWARDA liczonej przy starcie)
#  - status_setupu: + 'BEZ BRAMKI' (tryb 'etykieta': do handlu mimo typu bez zatwierdzenia) - zeby nie wygladal jak PROBA
#  - kontrola przy kazdym uruchomieniu: status_setupu vs do_handlu (niezgodnosci -> raport + run_meta)
#  - pewnosc_przewagi (tylko informacja): 'brak danych' < 20 okazji; 'niska' 20-49 okazji albo t < 1;
#    'umiarkowana' >= 50 okazji i t 1-2; 'wysoka' >= 50 okazji i t >= 2. PROBA przy 20-49 okazjach = 'niska'
#
# Co nowego w V6.22 (19.10 - tryb probny tylko z wykazana przewaga):
#  - PROBA wymaga: ocena A/B + typ+kierunek(+wejscie) ma po ustaleniu regul >= PROBA_MIN_OKAZJE UNIKALNYCH
#    rozstrzygnietych okazji (po filtrach) + srednia przewaga nad losowym wejsciem > 0 (+ t >= 0 od 50 tr.).
#    Ponizej progu albo bez danych -> nie do handlu (wczesniej 0-19 tr. wystarczalo A/B). Jedna funkcja proba_ok()
#  - status_setupu: DO GRY (typ zatwierdzony) / PROBA (tryb probny) / OBSERWACJA (setup OK, typ bez wykazanej
#    przewagi) / ZABLOKOWANY (inne powody: tryb ochronny, alarm BTC, filtry, ocena D, rezim ...); powod w 'warunki'
#    ranking_market.csv / ranking_limit.csv (osobno, z ocena A-D) + status_setupu
#  - TEST_BRAMKI: syntetyczne przypadki bramki/proby przy kazdym uruchomieniu (0/19/20/50 okazji, przewaga ujemna,
#    brak danych, ocena C, tryb ochronny, typ DO GRY, alarm BTC) - blad testu przerywa skrypt
#
# Co nowego w V6.21 (19.10):
#  - TRYB PROBNY wymaga dodatniej przewagi po ustaleniu regul: typ+kierunek(+wejscie) z >= PROBA_MIN_N_EDGE tr. po
#    ustaleniu regul musi miec srednia przewage nad losowym wejsciem > 0 (ponizej tej liczby: tylko A/B jak dotad)
#  - jakosc_testu.csv: dodatkowo kazda strategia osobno (typ_wersja x LONG/SHORT) - po filtrach i po ustaleniu regul
#  - hist_summary.csv: + po_reg_n / po_reg_R / po_reg_edge (30 dni rozdzielone na probe projektowa i po ustaleniu regul)
#  - RAPORT: ostrzezenie o nakladaniu sie testow (test 7 dni, archiwum i historia 30 dni obejmuja te same dni -
#    to NIE sa niezalezne potwierdzenia) + zakresy dat; przy kazdym zrodle: transakcje / okazje / zdjecia
#  - OI: bez zmian w logice (WEEX raportuje zawyzone OI bez stalego mnoznika - test 19.10); qc.json nadal zglasza
#    oi_niewiarygodne, summary.csv ma oi_vs_gate i oi_wiarygodnosc; OI nie wplywa na ocene ani na probe
#
# Co nowego w V6.20 (19.10 - porzadek w trybie probnym):
#  - status_bramki 'PROBA' dla setupow dopuszczonych w trybie probnym (wczesniej 'OBSERWACJA' jak niedopuszczone);
#    'DO GRY' zostaje tylko dla typu+kierunku zatwierdzonego przez bramke. Raport: osobna etykieta [PROBA]
#  - PROBA_MAX_NA_RAZ: najwyzej tyle setupow PROBA do handlu w jednym raporcie (najlepsze wg score_R / oceny / RR),
#    pozostale: 'limit proby'. PROBA_MAX_TYP_N_UJEMNY: typ+kierunek, ktory po ustaleniu regul ma juz >= tyle transakcji
#    i ujemna przewage nad losowym wejsciem (t < 0), nie wchodzi do proby (None = wylaczone)
#  - DZIENNIK PROBY (proba_log.csv na Drive, kopia w zipie): kazdy setup PROBA z chwila wystawienia; przy kolejnych
#    uruchomieniach rozliczany tym samym simulate() na nowych swiecach 5m (do przodu, bez zagladania w przyszlosc).
#    proba_wyniki.csv + linia w raporcie: n, rozstrzygniete, sr. R po kosztach - czy proba ma przewage
#  - zone_ref nie jest juz zaokraglane do ticka (to poziom odniesienia, nie cena zlecenia)
#
# Co nowego w V6.19 (19.10 - TRYB PROBNY bramki):
#  - BRAMKA_TRYB: 'twardy' (V6.18: tylko DO GRY), 'probny' (domyslny), 'etykieta' (do V6.17: bramka nic nie blokuje)
#    'probny': typ+kierunek w OBSERWACJI moze byc do handlu, ale TYLKO z ocena A lub B i z ryzykiem x PROBA_MNOZNIK
#    (laczy sie z trybem ochronnym: x0.5 x0.5 = x0.25). Typ ze statusem DO GRY - pelne ryzyko, ocena jak dotad.
#    Pozostale warunki bez zmian: tryb ochronny strony, alarm BTC, dzienny limit, rezim, BTC przeciw, filtry
#
# Co nowego w V6.18 (19.10):
#  - BRAMKA_TWARDA = True: 'do handlu' tylko dla typu+kierunku (+ wejscia), ktory po ustaleniu regul ma >= 50 tr.,
#    sr. >= +0.05R, przewage nad losowym wejsciem >= +0.05R i t przewagi >= 1. Stan 19.10: zaden typ -> nic do handlu
#  - SPRAWDZONE I NIE ZMIENIONE (19.10, archiwum po 15.10, pozostale filtry bez zmian):
#    * SWING4H SHORT (wylaczony): +0.35R / 89 tr., ale przewaga nad losowym shortem tylko +0.03R, t -0.48 -> zysk
#      z samego spadku rynku, nie z setupu; w 30 dniach p1 -0.26R, p2 +0.50R - zostaje wylaczony
#    * PULLBACK MARKET SHORT (wylaczony): +0.051R, przewaga 0.10, t 1.73 - na granicy progu, LIMIT SHORT -0.04R;
#      zostaje wylaczony (typy wylaczone nie wracaja przez sama bramke)
#
# Co nowego w V6.17 (19.10 - walidacja tylko poza proba projektowa, BEZ zmian w setupach):
#  - BRAMKA osobno dla typ_wersja x KIERUNEK (wejscie MARKET/LIMIT juz rozdzielone przez .M), liczona TYLKO na zdjeciach
#    od max(GATE_FROM_UTC, pierwsze uruchomienie tej wersji na zywo, okno GATE_WINDOW_DAYS) - bez mieszania wynikow
#    sprzed i po ustaleniu regul. Wczesniej LONG i SHORT liczyly sie razem (slaba strona chowala sie za mocna)
#  - gate.csv: + side, unikalne_okazje; setupy: po_reg_n / po_reg_R / po_reg_t / po_reg_okazje / po_reg_od
#    (wynik typu+kierunku po ustaleniu regul) obok hist_* (30 dni, glownie w probie - tylko informacja)
#  - BRAMKA_TWARDA (domyslnie False = jak dotad: status bramki to etykieta, nie blokuje 'do handlu').
#    True = 'do handlu' tylko dla typu+kierunku ze statusem DO GRY
#  - hist_summary.csv: + unikalne_okazje
#  - OI 'niezgodne z Gate': oi_usdt nie jest pokazywane (przeniesione do oi_surowe_usdt); prog MIN_OI bez zmian
#
# Co nowego w V6.16 (19.10 - uczciwosc testu, BEZ zmian w setupach i wynikach R):
#  - NIEJEDNOZNACZNE SWIECE (kolumna 'niejednoznaczne'): z OHLC nie wiadomo, co bylo pierwsze, gdy w jednej swiecy 5m
#    sa: wejscie LIMIT + SL ('wejscie+SL', liczone SL), wejscie + TP1 ('wejscie+TP', TP pominiety), SL + TP1 ('SL+TP',
#    liczone SL), SL + poziom 1R ('SL+1R', liczone SL bez czesci). Wynik r zostaje PESYMISTYCZNY jak dotad;
#    jakosc_testu.csv pokazuje obok sredni wynik bez niejednoznacznych (porownanie, nie wybor lepszego wariantu)
#  - LUKA DANYCH W TRAKCIE (status 'luka_danych'): brak swiec 5m miedzy zdjeciem a wyjsciem/wygasnieciem -> wynik
#    nierozliczony (poza FINAL: nie liczy sie do trybu ochronnego, bramki, oceny historii); status_przed_luka zostaje
#  - STABILNE ID: okazja_id = symbol + typ + kierunek + strefa wejscia (ta sama okazja niezaleznie od MARKET/LIMIT
#    i wersji), event_id = okazja_id + typ_wersja + rodzaj wejscia. W backtest.csv, archiwum i hist_backtest.csv
#  - jakosc_testu.csv + sekcja w raporcie: dla testu 7 dni, archiwum i historii 30 dni (wszystko oraz MARKET/LIMIT
#    x LONG/SHORT): wykryte, unikalne zdarzenia, unikalne okazje, rozstrzygniete, niejednoznaczne, z luka danych,
#    sredni R pesymistyczny i bez niejednoznacznych (po filtrach), niezalezne zdjecia rynku (t liczony po zdjeciach),
#    PO USTALENIU REGUL: wiersze ze zdjec od max(GATE_FROM_UTC, pierwsze uruchomienie na zywo danej wersji typu)
#    - tylko te nie byly uzyte do strojenia regul (reszta = w probie)
#  - OI JAWNIE: summary.csv ma oi_api_surowe (wartosc z API), oi_jednostka_zalozona ('ilosc bazowa'), oi_mnoznik (1),
#    oi_contractVal (do wgladu - NIE mnozony: test 19.10, z mnozeniem dalej od Gate w 50/62), gate_oi_usd, oi_vs_gate,
#    oi_wiarygodnosc: 'zgodne z Gate' (0.2-5x) / 'niezgodne z Gate' / 'bez potwierdzenia'. OI nie wplywa na ocene A-D
#    (tak bylo i jest); prog MIN_OI_USDT dla tradable zostaje (test 19.10 na archiwum: brak zgodnosci w polowach)
#
# Co nowego w V6.15 (19.10 - polaczenie z V6.56 collector: przeniesione tylko elementy techniczne + test reszty):
#  - CENY DO TICKA GIELDY (pricePrecision z exchangeInfo): entry/TP do najblizszego ticka, SL ZAWSZE na zewnatrz
#    (dalej od wejscia), TP1/TP2 w strone wejscia - ceny gotowe do wpisania na WEEX (bylo: 7 cyfr znaczacych)
#  - JAKOSC SWIEC: wiersze z blednym OHLC (high < max(open,close), low > min, cena <= 0) usuwane; luka w swiecach 5m
#    z ostatnich DANE_LUKI_H godzin -> tradable False ('luki w danych'), wpis w qc.json
#  - ZEGAR: przesuniecie czasu serwera liczone od srodka zapytania (polowa RTT), jak w V6.56
#  - ODRZUCONE PRZY BUDOWIE: setup wykryty, ale odrzucony w finalize (RR < min, SL za szeroki, LIMIT za daleko,
#    cena za TP1 ...) -> odrzucone.csv z powodem + licznik w raporcie (V6.56: near_misses / rejection_analysis)
#  - SPRAWDZONE I NIE PRZENIESIONE z V6.56 (test 19.10 na 30 dniach i archiwum WEEX):
#    * potwierdzenie OI (OI rosnie + cena w strone transakcji): archiwum 242 tr. -0.43R vs -0.13R bez - gorzej
#    * trend 15m i 1h zgodne jako warunek: 30 dni p2 -0.15R vs +0.01R, archiwum p2 -0.08R vs -0.26R - brak zgodnosci
#    * TP1 ograniczony do 1.5R / najblizszego pivota: 30 dni p1 +193R vs +225R, p2 -89R vs -96R - brak zgodnosci
#    * RVOL (wolumen swiecy / srednia) - juz jest jako wolumen_skok (hist_cechy.csv), w archiwum brak zgodnosci
#    * ocena punktowa V6.56 (trend/rvol/oi/spread...) bez testu historycznego - zostaje ocena V6.14 + test 30 dni
#    * dwa etapy pobierania (top 30 dokladnie) - test 30 dni potrzebuje pelnych danych wszystkich kontraktow
#    * shadow ledger - to samo robi backtest_archive.csv (wyniki kazdego setupu z kolejnych uruchomien)
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
# =====================================================================================

# ==================================== USTAWIENIA ====================================
TOP_N = 170                     # V6.14: ile kontraktow KRYPTO (wg wolumenu 24h); ponizej MIN_QUOTE_VOL_USDT nie sa pobierane
TRADFI_ON = True                # V6.14: osobna pula TradFi (akcje, indeksy, Pre-IPO, metale, surowce)
TRADFI_N = 60                   # ...ile kontraktow TradFi wg wolumenu (15.10: 30 -> 60 na prosbe; cienkie i tak 'nie do handlu')
TRADFI_MIN_N = 20               # ...filtr 'tradfi_bez_testu' zdejmowany od tylu transakcji typu+kierunku+wejscia w tescie akcji
TRADFI_MIN_T = 1.0              # ...i przy t przewagi >= tyle (oraz sredni wynik > 0, przewaga > 0)
MALE_PARY_RANK = 100            # pary spoza top N wg wolumenu: w ocenie -1 pkt (plytszy book, wiecej przypadkowych ruchow)
ALWAYS_INCLUDE = ['BTCUSDT', 'ETHUSDT']
# V6.36: hipoteza zapisana przed testem (NIE zmieniac progow po fakcie - wtedy test traci sens)
HIPOTEZA_OD = '2026-10-20T08:00'      # liczone tylko zdjecia od tej chwili (UTC)
HIP_MIN_ZDJEC = 20                    # V6.37: min. niezaleznych zdjec rynku
HIP_MIN_OKAZJI = 30                   # V6.37: min. unikalnych okazji
HIP_MIN_N_WEJSCIE = 25                # V6.37: min. transakcji osobno dla LIMIT i dla MARKET
HIPOTEZA_BB_RANK_MAX = 25             # kompresja: bb_rank_1h < 25 (dolna 1/3 z okresu 19.09-04.10) lub squeeze_1h
# V6.34: lewarowane ETF-y (2x/3x, long i short) na najbardziej ruchliwe akcje i indeksy - pobierane zawsze, jesli sa na WEEX
WATCH_TRADFI = ['TSLL', 'TSLQ', 'NVDL', 'NVDU', 'NVDX', 'NVDQ', 'MSTU', 'MSTX', 'MSTZ', 'CONL', 'CONX',
                'PLTU', 'AMDL', 'METU', 'AVL', 'TSMX', 'SMCX', 'HOOX', 'RKLX', 'IONX',
                'MUU', 'SOXL', 'SOXS', 'TQQQ', 'SQQQ', 'SPXL', 'UPRO', 'SPXU', 'TNA', 'FNGU', 'LABU', 'KORU']
WATCH_TRADFI_MAX = 15           # V6.34: maks. ile kontraktow z listy (wg wolumenu) dodac ponad top TradFi
TAKER_FEE_BPS = 8.0             # zapasowo, gdy brak w danych kontraktu (WEEX: 0.08%)
MAKER_FEE_BPS = 2.0             # zapasowo (WEEX: 0.02% dla wiekszosci kontraktow)
ORDER_SIZE_USDT = 1000          # wielkosc zlecenia do szacowania poslizgu
SETUP_MIN_RR = 1.5              # min. R:R netto do TP1
MAX_SPREAD_BPS = 10             # tradable: max spread
MIN_DEPTH_USDT = 25_000         # tradable: min. glebokosc booka +-0.5%
MIN_QUOTE_VOL_USDT = 500_000    # tradable: min. wolumen 24h (bylo 3M - na WEEX tylko ~100 par ma >= 3M)
MIN_OI_USDT = 1_000_000         # tradable: min. open interest
OI_PROG_TYLKO_Z_GATE = True     # V6.68: prog OI blokuje tylko, gdy Gate potwierdza OI < progu (False = jak V6.67)
MIN_OI_TRADFI_USDT = 0          # V6.35: OI TradFi tylko informacja (bylo 10k, wczesniej 100k); V6.14: prog OI dla TradFi (akcje maja na WEEX male OI); spread, glebokosc i wolumen bez zmian
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
# V6.136: OOS nie posiada historycznych snapshotow orderbook. Stosuj jawny,
# konserwatywny koszt staly zamiast dzisiejszej plynnosci (brak lookahead book).
# V6.150: staly 30 bps (V6.136) byl ok. 5-8x wiekszy niz realny poslizg (mediana z booka 3-6 bps) i obnizal
# kazdy wynik testu srednio o ~0.15R (najmocniej setupy z ciasnym SL) -> bramka, tryb ochronny i hist_* zanizone.
# Teraz: progi wg WOLUMENU 24h kontraktu W CHWILI ZDJECIA (ze swiec 1h, as-of - bez dzisiejszego booka i lookahead),
# ustawione konserwatywnie na ok. 90. percentyl poslizgu z booka w danej grupie wolumenu.
HISTORICAL_SLIP_PROGI = ((50e6, 5.0), (10e6, 10.0), (0.0, 12.0))   # (min wolumen 24h USDT, poslizg bps)
HISTORICAL_SLIP_BPS = 15.0      # brak danych o wolumenie w chwili zdjecia
HISTORICAL_SLIP_MODEL = 'TIER_QV24_ASOF_V150'
LIMIT_PRZEBICIE_BPS = 2.0       # V6.11: LIMIT wypelniony dopiero, gdy cena przejdzie przez wejscie o tyle bps (0 = dotkniecie)
FUNDING_W_WYNIKU = True         # V6.11: stawki fundingu w czasie trzymania pozycji wliczone w wynik testu (funding_R)
RANK_DAYS = 7                   # ranking: wyniki typu+kierunku+wejscia z archiwum z ostatnich N dni
RANK_SHRINK_K = 30              # sciaganie sredniej do zera: wynik * n / (n + K) - mala proba = slaby sygnal
USE_DRIVE = True                # archiwum testu i historia OI na Google Drive
BACKTEST = True                 # test wsteczny setupow na pobranych swiecach
BT_LOOKBACK_H = 168             # ile godzin wstecz siega test (ogranicza go historia 5m)
BT_STEP_H = 2                   # co ile godzin "zdjecie" rynku w tescie
BT_CACHE = True                 # V6.98: pamiec analizy testu wstecznego miedzy uruchomieniami (wyniki identyczne)
BT_CACHE_FILE = 'bt_cache_v699.json.zlib'
BT_MAX_HOLD_H = 12              # max czas trzymania pozycji w tescie
BT_ARCHIVE_DAYS = 30            # ile dni trzymac w archiwum
HISTORY_5M_BARS = 2600          # ile swiec 5m pobrac (po 1000 na zapytanie; ~9 dni)
# --- typy setupow i bramka skutecznosci
SETUP_TYPES = {'REV_MSS': 4, 'CONT_SWEEP': 4, 'PULLBACK': 4, 'SWING4H': 2, 'CROWD': 2, 'ODBICIE': 2}   # typ: wersja definicji (zmiana = nowa statystyka)
                                # V6.4: typy z wejsciem w FVG -> nowa wersja (wejscie na krawedzi FVG)
                                # V6.5: CONT_SWEEP.v4 (SHORT 0.15 ATR przed poziomem)
SETUP_WAIT_H = {'REV_MSS': 2, 'CONT_SWEEP': 3, 'PULLBACK': 4, 'SWING4H': 24, 'CROWD': 3, 'ODBICIE': 0}
SETUP_HOLD_H = {'SWING4H': 72, 'ODBICIE': 48}   # V6.56: ODBICIE 24 -> 48 h  # max czas trzymania wg typu (reszta: BT_MAX_HOLD_H)  # waznosc zlecenia LIMIT
GATE_FROM_UTC = '2026-10-15T08:00'  # V6.10: nowy filtr short_godziny. Wczesniej 2026-10-14T16:00 - do bramki licza sie tylko zdjecia rynku od tej chwili (V6.4: reguly z 14.10 dopasowane do danych do 15:18 UTC)
GATE_WINDOW_DAYS = 14           # bramka liczona z ostatnich N dni
BRAMKA_TRYB = 'probny'          # V6.19: 'twardy' / 'probny' / 'etykieta' (opis w naglowku)
PROBA_MNOZNIK = 0.5             # V6.19: ryzyko setupu typu w OBSERWACJI w trybie probnym
PROBA_MAX_NA_RAZ = 2            # V6.20: max setupow PROBA do handlu w jednym raporcie
PROBA_MIN_N_WYNIK = 20          # V6.52 (V6.40: 50): od tylu okazji proba wymaga sr. wyniku netto po ustaleniu regul >= 0
PROBA_MAX_TYP_N_UJEMNY = 50     # V6.20: typ z >= 50 tr. po ustaleniu regul i t przewagi < 0 nie wchodzi do proby
PROBA_MIN_N_EDGE = 20           # V6.21 (w V6.22 zastapione przez PROBA_MIN_OKAZJE)
PROBA_MIN_OKAZJE = 20           # V6.22: min. unikalnych rozstrzygnietych okazji po ustaleniu regul dla PROBA
PROBA_Z_BADANIA = ('ODBICIE',)  # V6.60: typy dopuszczone do PROBY przed PROBA_MIN_OKAZJE (przewaga z badania 39 dni)
PROBA_BADANIE_STOP_N = 10       # V6.60: ...ale od tylu okazji na zywo sredni wynik < 0 -> z powrotem OBSERWACJA
OCHRONA_BEZ_TYPOW = ('ODBICIE',)   # V6.60: tryb ochronny strony nie blokuje i nie zmniejsza ryzyka tych typow
ROZB_MIN_N = 20                 # V6.24: min. transakcji po ustaleniu regul do ostrzezenia o rozbieznosci
TEST_BRAMKI = True              # V6.22: syntetyczny test bramki przy starcie
ILOSC_MIN_MAX_RYZYKO = 1.5      # V6.32: min. pozycja moze ryzykowac najwyzej tyle x zalozone ryzyko
PROBA_LOG = 'proba_log.csv'     # V6.20: dziennik setupow PROBA na Drive (rozliczany do przodu)
BRAMKA_TWARDA = BRAMKA_TRYB == 'twardy'   # V6.18: True = 'do handlu' tylko gdy bramka typu+kierunku = DO GRY
GATE_MIN_TRADES = 50            # min. rozstrzygnietych transakcji typu
GATE_WAGA_ZDJECIA = True        # V6.72: progi sredniego R i przewagi liczone z waga zdjecia rynku (srednia wynikow zdjec),
                                # nie po transakcjach - zdjecie z 30 skorelowanymi sygnalami nie liczy sie 30x (t juz tak liczone)
GATE_MIN_AVG_R = 0.05           # min. sredni wynik na transakcje (R, po kosztach)
GATE_MIN_T = 1.0                # min. t-stat PRZEWAGI (edge) liczony po zdjeciach rynku
GATE_MIN_EDGE_R = 0.05          # min. srednia przewaga nad losowym wejsciem w tym samym kierunku (R)
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
HIST_REFRESH_H = 24             # (V6.51: zastapione oknem ponizej; zostaje jako minimalny odstep w oknie = 12 h)
HIST_OKNO_UTC = (2, 6)          # V6.51: test 30 dni liczony od nowa tylko w tych godzinach UTC (od, do) - raz na dobe
HIST_REFRESH_MAX_H = 36         # V6.51: awaryjnie od nowa, gdy cache starszy niz tyle godzin (np. brak porannego przebiegu)
HIST_WARIANTY = {               # V6.4: te same 30 dni policzone z innymi ustawieniami -> hist_warianty.csv (A/B bez zgadywania).
    'cont_sl_0.8': {'CONT_SL_ATR': 0.8},                       # V6.71: kontrola - stare ustawienie (domyslnie teraz 1.2)
    'market_zone_0.15': {'MARKET_ZONE_ATR': 0.15},             # V6.50: MARKET tylko blizej strefy
    'cont_long_0.05': {'CONT_ENTRY_FRONT_ATR': 0.05},          # V6.57: stare ustawienie (kontrola)
    'odbicie_v1': {'ODBICIE_SPADEK_PCT': 12.0, 'ODBICIE_TP_ATR': 4.0,                       # V6.56: stare reguly v1
                   'SETUP_HOLD_H': {'SWING4H': 72, 'ODBICIE': 24}},
    'odbicie_po_pompie': {'ODBICIE_PO_POMPIE_PCT': 30.0},      # V6.56: filtr po pompie (V6.54)
}                               # 23.10 usuniete: sl_bufor_0.25 (-19.0R / -9.6R), fvg_0.75 (-17.0R / -3.7R),
                                # market_zone_0.10 (sredni R gorszy niz baza)                               # 19.10 odrzucone (to samo okno): koszt_max_0.15 (-4.1/-40.2R), azja_short_stary (+2.4/-8.6R),
                                # sl_max_8pct (-5.7/+7.3R)                               # 17.10 odrzucone: trail_2R_1R (-0.1R / 1934 tr.), trail_schodki (+0.9R) - p1 gorzej, p2 lepiej = brak efektu                               # odrzucone 14.10: FVG_tylko_swiezy, CONT_bez_retestu, CONT_LONG_0.15ATR, CONT_SHORT_0.05                               # kazdy wariant ~ +2-3 min raz na HIST_REFRESH_H; {} = wylaczone. Zmiana domyslnych dopiero,
                                # gdy wariant jest lepszy w OBU polowach 30 dni (kolumny p1/p2)
# --- dzwignia i wielkosc pozycji (margin ISOLATED) ---
KAPITAL_USDT = 200.0            # 21.10 22:07: stan konta po zamknieciu pozycji (aktualizuj co kilka dni). 1.5% = 3.0 USDT
RYZYKO_NA_TRANSAKCJE_PCT = 1.5  # V6.44 (bylo 1.0): ile % kapitalu tracisz, gdy wejdzie SL (z prowizjami i poslizgiem)
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
OCENA_RR_PKT = 0                # V6.48: punkt za R:R >= 2.5 (bylo +1; w archiwum WEEX gorsze w 3/3 okresach)
OCENA_PROGI = {'A': 2, 'B': 1, 'C': 0}   # punkty strukturalne: >=2 A, 1 B, 0 C, mniej = D (pomin)
OCENA_WAGA_HISTORII = 0         # punkty za historie typu (0 = tylko informacja). Test 14.10: historia 30d nie przewidywala
                                # kolejnych 15 dni (przewaga PULLBACK LONG pochodzila z jednego wzrostowego tygodnia)
WYLACZONE_TYPY = {('PULLBACK', 'SHORT'), ('REV_MSS', 'SHORT'),   # tracily w obu polowach 30 dni -> filtr typ_wylaczony
                  ('SWING4H', 'LONG'), ('SWING4H', 'SHORT'),       # V6.5: SWING4H po filtrach ujemny w obu polowach 30 dni
                  ('PULLBACK', 'LONG')}                            # V6.71: ujemny w obu polowach 30 dni i w obu tygodniach na zywo
WYLACZONE_WEJSCIA = {('CONT_SWEEP', 'LONG', 'LIMIT')}              # V6.71: (typ, kierunek, wejscie) - CONT_SWEEP LONG LIMIT
                                                                   # ujemny w 3 z 4 okien (30 dni p1/p2, tygodnie), w 4. ~0


def typ_wylaczony(typ, side, entry_type=None):
    """V6.71: typ+kierunek z WYLACZONE_TYPY albo typ+kierunek+wejscie z WYLACZONE_WEJSCIA."""
    return (typ, side) in WYLACZONE_TYPY or (typ, side, entry_type) in WYLACZONE_WEJSCIA
# --- V6.53: ODBICIE (LONG po duzym spadku 4h) ---
ODBICIE_SPADEK_PCT = 10.0       # V6.56 (v1: 12)       # spadek ceny w 4h (od otwarcia swiecy 5m sprzed 4h) co najmniej tyle %
ODBICIE_SL_ATR = 2.0            # SL = wejscie - tyle ATR 1h
ODBICIE_TP_ATR = 8.0            # V6.56 (v1: 4)            # TP = wejscie + tyle ATR 1h (staly, bez celow plynnosci)
ODBICIE_KLUCZ_H = 12            # okazja = szczyt z tylu godzin (badanie: jedna transakcja na coin na 12 h)
ODBICIE_MAX_RISK_PCT = 25.0
ODBICIE_PO_POMPIE_PCT = None    # V6.56: wylaczony (V6.54: 30)    # V6.54: wzrost w 24h przed spadkiem powyzej tylu % -> filtr 'po_pompie'     # max odleglosc SL w % ceny (zamiast SETUP_MAX_RISK_PCT)
BEZ_1R_TYPY = {'ODBICIE'}       # typy bez zamykania czesci na +1R (trzymane do TP/SL/czasu)
TYLKO_MARKET_TYPY = {'ODBICIE'} # typy tylko z wejsciem po rynku
OCENA_STALA = {'ODBICIE': 'B'}  # typy z ocena stala (punkty A-D nie dotycza; nadpisania bezpieczenstwa dzialaja)
ZARZADZANIE_1R = True           # 50% pozycji zamykane przy +1R, reszta z SL na wejsciu (breakeven); test 14.10: lepiej w obu polowach 30 dni
CZESC_1R = 0.5                  # jaka czesc zamknac przy 1R
TRAIL_PO_1R_R = None            # opcja: po 1R reszta z trailing stopem (SL = najlepsza cena - tyle R, nie gorzej niz wejscie).
                                # Test 14.10 (replay, 1312 tr.): 1.0R -0.04R/tr., 0.5R -0.09R/tr. vs SL na wejscie -> wylaczone
TRAIL_STOPNIE = None            # V6.14 (test): po 1R stop schodkowo, np. ((2, 1), (3, 2)) = po +2R SL na +1R, po +3R na +2R.
                                # None = jak dotad (po 1R SL na wejsciu). Wlaczyc dopiero po wyniku w hist_warianty.csv (obie polowy)
HOLD_PO_1R_H = None             # opcja: po 1R trzymaj do tylu godzin (inaczej BT_MAX_HOLD_H). Test 14.10: 24h = bez roznicy (+0.004R)
FILTR_AZJA_SHORT = False        # V6.4: brak SHORT w sesji azjatyckiej (00-07 UTC). V6.10: wylaczony, zastapiony przez FILTR_SHORT_GODZINY_UTC
FILTR_SHORT_GODZINY_UTC = (2, 3)   # V6.10: brak SHORT o tych godzinach UTC (02-03 UTC = 04-05 PL); () = wylaczone
DANE_LUKI_H = 6                 # V6.15: luka w swiecach 5m z ostatnich tylu godzin -> 'luki w danych' (tradable False)
CENY_DO_TICKA = True            # V6.15: entry/SL/TP zaokraglone do ticka WEEX (pricePrecision); SL na zewnatrz
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
MAX_RYZYKO_KIERUNEK_PCT = 4.5   # V6.47: 3 pozycje x 1.5% (V6.44: 3.0, wczesniej 2.0): ostrzezenie: laczne ryzyko setupow A/B w jednym kierunku powyzej tylu % kapitalu
CROWD_Z = 2.5                   # CROWD: |funding z| od tylu = tlum mocno po jednej stronie (z historii fundingu do danej chwili)
CROWD_TURN_BARS = 8             # CROWD: zwrot struktury 15m przeciw tlumowi nie starszy niz tyle swiec (2h)
CROWD_EXT_BARS = 32             # CROWD: SL za ekstremum ostatnich tylu swiec 15m (8h)
KSIAZKA_LOG = 'ksiazka_log.csv' # V6.61: na Drive: cechy arkusza zlecen kazdego setupu (do testu po ~tygodniu)
KSIAZKA_SCIANA_X = 5.0          # V6.61: sciana = poziom >= tyle x mediana wartosci poziomu w arkuszu (opis w raporcie)
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

import bisect
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
from decimal import Decimal, ROUND_HALF_UP, ROUND_FLOOR, ROUND_CEILING
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

# SZYBKI SKAN: odklada kosztowne, czysto badawcze symulacje portfela.
# Aby uzyskac wszystkie raporty porownawcze, ustaw False i uruchom osobno.
FAST_SCAN = False
VERSION = 'V6.183'
# V6.118: ograniczony audyt historyczny US30, tylko diagnostyka (max 2 API requests).
ZERO_US30_VERIFY = True
ZERO_US30_MAX_REQUESTS = 2
WEEX = 'https://api-contract.weex.com'
INTERVALS = {'5m': HISTORY_5M_BARS, '15m': 1000, '1h': 1000, '4h': 500}
KLINES_MAX_LIMIT = 1000
KLINES_FALLBACK_LIMIT = 500
FUNDING_LIMIT = 200              # V6.8: 200 stawek (~66 dni przy 8h) - test 30 dni ma funding na calym okresie
FUNDING_LIMIT_FALLBACK = 30
FUNDING_Z_LIVE_N = 30           # funding z na zywo: z tylu ostatnich stawek (jak przed V6.8)
DEPTH_LIMITS = (200, 15)
WORKERS = 16                 # V6.108: ukrywanie opoznien sieci; limit 10 req/s bez zmian
MAX_REQ_PER_SEC = 10
TIMEOUT = 20
RETRIES = 4
DRIVE_DIR = '/content/drive/MyDrive/weex_collector'
ARCHIVE_NAME = 'backtest_archive_v63.csv'   # V6.4 zmienia tylko filtry (wynik w R bez zmian) -> archiwum V6.3 dalej wazne
OI_HISTORY_NAME = 'oi_history_v63.json'   # OI w USDT; plik V6.1 (oi_history.json) mial inne jednostki
OI_HISTORY_KEEP_DAYS = 14
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
CONT_ENTRY_FRONT_ATR = 0.0      # V6.57 (bylo 0.05): LIMIT tyle ATR przed poziomem (retest nie zawsze dotyka idealnie) - LONG
CONT_ENTRY_FRONT_ATR_SHORT = 0.15   # V6.5: SHORT (test 30 dni: lepiej w obu polowach); None = jak CONT_ENTRY_FRONT_ATR
CONT_MAX_RETESTY = None         # V6.5: max swiec 15m po wybiciu, ktore juz dotknely wejscia (None = bez filtra, tylko kolumna)
CONT_SL_ATR = 1.2               # V6.71 (bylo 0.8): SL za poziomem, gdy brak struktury 15m - wariant lepszy w obu polowach 30 dni
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
STOCK_TYPES = {'Stocks', 'Indices', 'Pre-IPO'}   # do V6.87 handel tylko w sesji USA
TRADFI_BEZ_BLOKAD = True        # V6.93: bez bramki krypto, lecz z twardymi kontrolami wykonania i ryzyka
TRADFI_24H = True               # V6.88: na WEEX akcje/indeksy/Pre-IPO sa handlowane cala dobe -> bez blokady sesji USA
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
        # Rezerwacja terminu pod blokada, ALE spanie poza nia: inne watki
        # moga zarezerwowac kolejne sloty bez czekania na ten watek.
        with self.lock:
            now = time.monotonic()
            slot = max(now, self.next_time)
            self.next_time = slot + self.interval
        delay = slot - time.monotonic()
        if delay > 0:
            time.sleep(delay)


limiter = RateLimiter(MAX_REQ_PER_SEC)


def get_session():
    if not hasattr(_thread_local, 'session'):
        s = requests.Session()
        s.headers.update({'User-Agent': 'Mozilla/5.0 market-analysis/6.3'})
        _thread_local.session = s
    return _thread_local.session


_api = {'n': 0}
_api_by_endpoint = {}  # V6.108: count, czas, kody HTTP, retry i opoznienia
_io_outcomes = {}  # V6.108: udane/bledne OI i arkusz (w tym awaryjny depth 15)
_short_history_verified = {}  # {(symbol, tf): earliest_ms} potwierdzone ostatnim pelnym pobraniem
_history_repairs = {}  # diagnostyka pobran dla symbol/interwal
_zero_sources = {}  # (symbol,tf,source): zerowe otwarcia z biezacego pobrania/wczytania
_zero_lock = threading.Lock()
_fetch_stats = {}  # V6.107: wyjasnia pelne, przyrostowe i pominiete pobrania
_funding_io = {}  # V6.107: status pamieci historycznego fundingu
_bt_cache_io = {'status': 'nie_uzyto', 'odczyt_s': 0.0, 'zapis_s': 0.0, 'rozmiar_b': 0, 'wpisy': 0}
_api_lock = threading.Lock()


def _api_stat(path, **changes):
    """Licznik bezpieczny dla watkow; nie zmienia polityki retry ani sygnalow."""
    with _api_lock:
        st = _api_by_endpoint.setdefault(path, {'count': 0, 'seconds': 0.0})
        for key, amount in changes.items():
            st[key] = st.get(key, 0) + amount


def _io_stat(name):
    with _api_lock:
        _io_outcomes[name] = _io_outcomes.get(name, 0) + 1


def request_json(base, path, params=None):
    url = base + path
    last_error = None
    for attempt in range(RETRIES):
        try:
            queue_start = time.monotonic()
            limiter.wait()
            _api_stat(path, rate_wait_s=time.monotonic() - queue_start)
            with _api_lock:
                _api['n'] += 1
            _http_t0 = time.monotonic()
            response = None
            try:
                response = get_session().get(url, params=params, timeout=TIMEOUT)
            finally:
                _http_sec = time.monotonic() - _http_t0
                _api_stat(path, count=1, seconds=_http_sec)
            status = response.status_code
            if status in (403, 451):
                _api_stat(path, forbidden=1)
                raise PermissionError(f'HTTP {status}: {url}. Dostep do API moze byc blokowany.')
            if status == 429 or status >= 500:
                _api_stat(path, retry_http=1, **{f'http_{status}': 1})
                last_error = f'HTTP {status}: {response.text[:200]}'
                retry_after = response.headers.get('Retry-After')
                try:
                    wait = float(retry_after) if retry_after else 1.5 * 2 ** attempt
                except (ValueError, TypeError):
                    wait = 1.5 * 2 ** attempt
                time.sleep(min(max(wait, 0), 20))
                continue
            if 400 <= status < 500:
                _api_stat(path, client_error=1)
                raise ValueError(f'HTTP {status}: {response.text[:200]}')
            response.raise_for_status()
            data = response.json()
            if isinstance(data, dict) and 'code' in data:
                code = str(data['code'])
                if code not in ('00000', '0', '200'):
                    _api_stat(path, exchange_error=1)
                    raise ValueError(f"WEEX code={code}, msg={data.get('msg')}, path={path}")
            _api_stat(path, success=1)
            return data
        except (PermissionError, ValueError):
            raise
        except Exception as exc:
            _api_stat(path, retry_exception=1)
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


def swiece_martwe(a5, n=6):
    """V6.88: ostatnie n swiec 5m bez wolumenu albo bez zakresu = rynek faktycznie zamkniety."""
    last = a5[-n:]
    return bool((last[:, V] <= 0).all() or (last[:, H] <= last[:, L]).all())


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
    """Czas serwera WEEX. V6.15: _clock['offset_ms'] liczony od srodka zapytania (polowa RTT), jak w V6.56."""
    try:
        t0 = time.time()
        st = int(weex('/capi/v3/market/time')['serverTime'])
        t1 = time.time()
        _clock['offset_ms'] = st - int((t0 + t1) / 2 * 1000)
        _clock['rtt_ms'] = int((t1 - t0) * 1000)
        return st + int((t1 - t0) / 2 * 1000)          # czas serwera w chwili odebrania odpowiedzi
    except Exception:
        return int(time.time() * 1000) + _clock.get('offset_ms', 0)


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


# V6.116: obserwacje surowych zer, bez jakiejkolwiek modyfikacji danych.
def _zero_times(rows, from_array=False):
    times = []
    for r in rows:
        try:
            if from_array:
                # Macierz archiwalna: kolumny O,H,L,C,V,...,T
                vals = [float(r[i]) for i in (O, H, L, C, V)]
                ts = int(float(r[T]))
            else:
                # WEEX: [openTime, open, high, low, close, ..., volume,...]
                vals = [float(r[i]) for i in (1,2,3,4,5)]
                ts = int(float(r[0]))
            if ts > 0 and all(math.isfinite(v) and v == 0 for v in vals):
                times.append(ts)
        except (ValueError, TypeError, IndexError, OverflowError):
            continue
    return sorted(set(times))


def _record_zeros(symbol, tf, source, rows, from_array=False):
    if source not in ('API', 'ARCHIWUM'):
        return
    points = _zero_times(rows, from_array)
    with _zero_lock:
        _zero_sources[(symbol, tf, source)] = points


def _zero_ranges(points, tf):
    # Grupuj tylko kolejne znaczniki interwalu: jedno zdarzenie zamiast wielu swiec.
    step = INTERVAL_MS[tf]
    runs = []
    for t in sorted(set(points)):
        if runs and t - runs[-1][1] == step:
            runs[-1][1] = t
            runs[-1][2] += 1
        else:
            runs.append([t, t, 1])
    return runs


def zero_provenance_rows():
    out = []
    for (sym, tf, src), pts in sorted(_zero_sources.items()):
        runs = _zero_ranges(pts, tf)
        out.append({'symbol': sym, 'interwal': tf, 'zrodlo': src,
                    'liczba_zerowych_swiec': len(pts), 'liczba_przedzialow': len(runs),
                    'przedzialy_json': json.dumps([
                        {'od_utc': iso(a), 'do_utc': iso(b), 'liczba': n}
                        for a,b,n in runs[:30]], ensure_ascii=False),
                    'przedzialy_pominiete': max(0, len(runs)-30)})
    return out



def zero_event_diagnostics(sources=None):
    """Scalaj nakladajace sie PRZEDZIALY CZASOWE zer OHLCV per symbol.

    Kazda swieca reprezentuje [open, open+interwal) w UTC. Rozne TF moga
    opisywac ten sam czas; unia przedzialow liczy go raz. Porownanie API i
    archiwum jest osobne per TF i wskazuje jedynie zgodnosc otwarc w danych
    aktualnie zaobserwowanych (brak obserwacji != brak zera historycznie).
    """
    src = _zero_sources if sources is None else sources
    by_symbol = {}
    by_pair = {}
    for (sym, tf, source), timestamps in src.items():
        if tf not in INTERVAL_MS or source not in ('API', 'ARCHIWUM'):
            continue
        pts = set(int(x) for x in timestamps if int(x) > 0)
        by_pair.setdefault((sym, tf), {})[source] = pts
        step = INTERVAL_MS[tf]
        for t in pts:
            by_symbol.setdefault(sym, []).append((t, t + step, tf, source))

    overlaps = []
    for (sym, tf), sources_by_tf in sorted(by_pair.items()):
        api = sources_by_tf.get('API', set())
        archive = sources_by_tf.get('ARCHIWUM', set())
        common = api & archive
        overlaps.append({'symbol': sym, 'interwal': tf,
                         'api_zerowe': len(api), 'archiwum_zerowe': len(archive),
                         'wspolne_otwarcia': len(common),
                         'tylko_api': len(api - archive),
                         'tylko_archiwum': len(archive - api),
                         'przypadki_wspolne_json': json.dumps([iso(t) for t in sorted(common)[:10]]),
                         'uwaga': 'Obserwacje z biezacego odczytu; brak wspolnych otwarc nie dowodzi roznicy danych historycznych.'})

    events = []
    for sym, intervals in sorted(by_symbol.items()):
        if not intervals:
            continue
        intervals.sort(key=lambda x: (x[0], x[1]))
        start = end = None
        tfs, origins = set(), set()
        records = 0
        def emit():
            return {'symbol': sym, 'od_utc': iso(start), 'do_utc_wylacznie': iso(end),
                    'czas_h': round((end-start)/3_600_000, 3),
                    'interwaly': ','.join(sorted(tfs)), 'zrodla': ','.join(sorted(origins)),
                    'rekordy_interwalowe': records, 'odrebne_zdarzenie': 1,
                    'us30': sym.upper() == 'US30USDT'}
        for a, b, tf, source in intervals:
            if start is not None and a > end:
                events.append(emit())
                start = end = None
            if start is None:
                start, end, records = a, b, 0
                tfs, origins = set(), set()
            end = max(end, b)
            records += 1
            tfs.add(tf)
            origins.add(source)
        if start is not None:
            events.append(emit())
    us30 = [r for r in events if r['us30']]
    summary = {'zdarzenia_unikalne': len(events),
               'symbole': len(by_symbol),
               'przedzialy_us30': len(us30),
               'czas_us30_h': round(sum(x['czas_h'] for x in us30), 3),
               'api_archiwum_wspolne_otwarcia': sum(x['wspolne_otwarcia'] for x in overlaps),
               'uwaga': ('Zakresy UTC sa polotwarte. Polaczenie przedzialow nie potwierdza '
                         'przyczyny zer: brak notowan, dane API i bledy archiwum pozostaja '
                         'odrebnymi hipotezami. Zera nie sa zastapione cenami.')}
    return events, overlaps, summary



def verify_us30_historical_zeros(replay=False, enabled=None, requester=None, sources=None):
    """Audyt historyczny max 2 zapytania dla US30 (1h/4h); NIE zmienia danych.

    Porownanie jest wazne tylko dla dokladnie tego samego openTime i interwalu.
    Brak timestampu w odpowiedzi (np. ignorowanie endTime) daje BRAK_DANYCH_POROWNAWCZYCH,
    nigdy ZERO_TYLKO_W_ARCHIWUM. Odczyt jest opcjonalny i pominiety w REPLAY.
    """
    if enabled is None:
        enabled = ZERO_US30_VERIFY
    src = _zero_sources if sources is None else sources
    ask = weex if requester is None else requester
    records = []
    requests_n = 0
    for tf in ('1h', '4h'):
        points = sorted(set(src.get(('US30USDT', tf, 'ARCHIWUM'), [])))
        if not points:
            continue
        observed = src.get(('US30USDT', tf, 'API'), [])
        # Dwa rozlaczne okna: jedno na kazdy interwal. Celujemy w historyczna
        # zerowa swiece, a nie w dzisiejsze notowania.
        anchor = points[len(points)//2]
        row = {'symbol':'US30USDT','interwal':tf,'punkt_kontrolny_utc':iso(anchor),
               'archiwum_zerowych_obserwowanych':len(points), 'api_biezace_zerowych':len(observed),
               'api_liczba_swiec':0, 'wspolne_czasy':0, 'zero_w_api':0,
               'niezero_w_api':0, 'brak_w_api':0, 'status':'BRAK_DANYCH_POROWNAWCZYCH',
               'uzasadnienie':'', 'zrodlo_porownania':'ODCZYT_HISTORYCZNY_WEEX',
               'api_od_utc':'', 'api_do_utc':'', 'proby_zakresu':0,
               'historyczny_zakres_pokryty':False, 'status_jakosci_zrodla':'BRAK_WERYFIKACJI'}
        if replay or not enabled:
            row['uzasadnienie'] = 'REPLAY: bez API' if replay else 'weryfikacja wylaczona'
            records.append(row)
            continue
        if requests_n >= ZERO_US30_MAX_REQUESTS:
            row['uzasadnienie'] = 'osiagnieto limit zapytan'
            records.append(row)
            continue
        # V6.120: zwykly /klines ignorowal endTime dla US30 1h i zwracal
        # najnowsze 500 swiec. Do historycznej weryfikacji 1h uzywamy
        # /historyKlines z OBOMA granicami czasu, tak jak hist_fetch().
        # Brak pokrycia okna = blad/ograniczenie API, a nie naprawione dane.
        combined = {}
        errors = []
        step = INTERVAL_MS[tf]
        if tf == '1h':
            start_t = int(anchor - 24 * step)
            end_t = int(anchor + 24 * step)
            path = '/capi/v3/market/historyKlines'
            params = {'symbol':'US30USDT', 'interval':tf,
                      'startTime':start_t, 'endTime':end_t, 'limit':100}
        else:
            path = '/capi/v3/market/klines'
            params = {'symbol':'US30USDT', 'interval':tf,
                      'limit':KLINES_FALLBACK_LIMIT,
                      'endTime':int(anchor + step - 1)}
        requests_n += 1
        row['proby_zakresu'] = 1
        row['endpoint_historyczny'] = path
        row['zadany_od_utc'] = iso(start_t) if tf == '1h' else ''
        row['zadany_do_utc'] = iso(end_t) if tf == '1h' else iso(params['endTime'])
        try:
            response = ask(path, params)
            for item in normalize_klines(unwrap(response)):
                try:
                    stamp = int(number(item[0]))
                    if stamp > 0:
                        combined[stamp] = item
                except (ValueError, TypeError, OverflowError, IndexError):
                    pass
        except Exception as e:
            errors.append(f'{type(e).__name__}: {str(e)[:120]}')
        row['api_ignoruje_zakres'] = bool(tf == '1h' and combined and
              not any(start_t <= t <= end_t for t in combined))
        row['api_liczba_swiec'] = len(combined)
        if combined:
            api_times = sorted(combined)
            row['api_od_utc'] = iso(api_times[0])
            row['api_do_utc'] = iso(api_times[-1])
            row['historyczny_zakres_pokryty'] = api_times[0] <= anchor <= api_times[-1]
        matching = [t for t in points if t in combined]
        row['wspolne_czasy'] = len(matching)
        for t in matching:
            if _zero_times([combined[t]]):
                row['zero_w_api'] += 1
            else:
                try:
                    vals = [float(combined[t][i]) for i in (1,2,3,4,5)]
                    if all(math.isfinite(v) for v in vals) and min(vals[:4]) > 0:
                        row['niezero_w_api'] += 1
                    else:
                        row['brak_w_api'] += 1
                except (TypeError, ValueError, IndexError, OverflowError):
                    row['brak_w_api'] += 1
        if row['zero_w_api']:
            row['status'] = 'ZERO_POTWIERDZONE_W_API'
            row['status_jakosci_zrodla'] = 'ZERO_ZRODLOWE_POTWIERDZONE'
            row['uzasadnienie'] = 'identyczny interwal i openTime: API nadal zwraca zerowe OHLCV'
        elif row['niezero_w_api']:
            row['status'] = 'ZERO_TYLKO_W_ARCHIWUM'
            row['status_jakosci_zrodla'] = 'ROZNICA_API_ARCHIWUM_DLA_POROWNANYCH'
            row['uzasadnienie'] = 'API zwraca poprawne OHLCV dla wspolnych znacznikow (tylko badana proba)'
        else:
            row['status_jakosci_zrodla'] = ('API_IGNORUJE_ZAKRES_HISTORYCZNY' if row.get('api_ignoruje_zakres') else 'NIEPOTWIERDZONE_BRAK_WSPOLNYCH_DANYCH')
            row['uzasadnienie'] = ('nie udało sie porownac tych samych timestampow; '
                                    'nie uznawac zer za naprawione' +
                                    ('; bledy: '+', '.join(errors) if errors else ''))
        records.append(row)
    summary = {'liczba_zapytan':requests_n,'porownane_interwaly':len(records),
               'potwierdzone_api':sum(r['status']=='ZERO_POTWIERDZONE_W_API' for r in records),
               'tylko_archiwum_w_probie':sum(r['status']=='ZERO_TYLKO_W_ARCHIWUM' for r in records),
               'bez_porownania':sum(r['status']=='BRAK_DANYCH_POROWNAWCZYCH' for r in records),
               'uwaga':'Wyniki obejmuja wylacznie zbadane znacznikami UTC swiece US30, nie caly zakres.'}
    return records, summary


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
    _record_zeros(symbol, tf, 'API', rows)
    return rows[-total:]


def need_bars(tf, n, known, now_ms):
    """Ile swiec pobrac (0 = wcale). known = (pierwsza, ostatnia) chwila otwarcia w archiwum.
    Wynik po sklejeniu z archiwum jest taki sam jak przy pelnym pobraniu, bo:
    - archiwum siega co najmniej tak daleko, jak pelne pobranie (bez stronicowania WEEX: max 1000 swiec),
    - nowe swiece + INCR_MARGIN_BARS zakladki nadpisuja koniec archiwum,
    - 15m/1h/4h: gdy archiwum ma juz ostatnia zamknieta swiece, nie ma czego pobierac (analiza uzywa tylko zamknietych)."""
    if not known or not INCREMENTAL_KLINES:
        return n
    first, last = known[:2]
    ms = INTERVAL_MS[tf]
    # Nie ufaj archiwum z uszkodzonym ogonem; swieze swiece pobierz ponownie.
    if len(known) > 2 and not known[2]:
        _fetch_count(f'klines_{tf}_naprawa_luki')
        return n
    full = n if _paging['supported'] else min(n, KLINES_MAX_LIMIT)   # V6.9: bez stronicowania pelne = max 1000
    cur = (now_ms // ms) * ms                       # otwarcie swiecy w trakcie
    if first > cur - (full - 1) * ms:
        # Dopiero potwierdzenie poprzednim pelnym odczytem, ze serwer nie
        # posiada starszych swiec, pozwala uniknac powtarzania tej samej historii.
        # W innym przypadku wymagamy pelnego odczytu (fail closed).
        if len(known) <= 3 or not known[3]:
            return n
        _fetch_count(f'klines_{tf}_naturalnie_krotka_historia')
    if tf != '5m' and last >= cur - ms:
        return 0                                    # V6.9: ostatnia zamknieta swieca juz w archiwum
    need = int((now_ms - last) // ms) + 1 + INCR_MARGIN_BARS
    if need > KLINES_MAX_LIMIT:
        _fetch_count(f'klines_{tf}_zbyt_duza_luka')
        return n
    return max(50, need)


def funding_cache_diagnostyka_v138(hist, now_ms):
    """Obserwacyjna diagnostyka ostatniego cyklu; nie przedluza TTL.

    Interwal jest MEDIANA ostatnich odstepow, NIE potwierdzonym harmonogramem
    przyszlych stawek; przy brakach lub duplikatach bezpiecznie odswiezamy.
    """
    result = {'aktualny': False, 'powod_szczegolowy': 'BRAK_LUB_NIEPOPRAWNA_HISTORIA',
              'odstep_typowy_ms': None, 'nastepny_szacowany_ms': None,
              'do_nastepnego_szacowanego_ms': None, 'wiek_ostatniej_stawki_ms': None,
              'zapas_bezpieczenstwa_ms': 120000, 'uwaga': 'Szacowanie z danych; nie jest to oficjalny harmonogram fundingu'}
    if not isinstance(hist, list):
        return result
    pts = sorted(int(number(x.get('fundingTime'))) for x in hist
                 if isinstance(x, dict) and number(x.get('fundingTime')))
    if len(pts) < 8:
        result['powod_szczegolowy'] = 'ZA_MALO_REKORDOW'
        return result
    # Zachowujemy oryginalna metode estymacji, aby NIE zmieniac decyzji API.
    gap = float(np.median(np.diff(pts[-6:])))
    last = pts[-1]
    result['wiek_ostatniej_stawki_ms'] = int(now_ms - last)
    if gap <= 0:
        result['powod_szczegolowy'] = 'NIEPOPRAWNY_ODSTEP'
        return result
    next_est = last + gap
    result.update(odstep_typowy_ms=gap, nastepny_szacowany_ms=int(next_est),
                  do_nastepnego_szacowanego_ms=int(next_est-now_ms))
    result['aktualny'] = bool(now_ms < next_est - 120000)
    result['powod_szczegolowy'] = ('CACHE_WAZNY_WG_SZACOWANEGO_CYKLU' if result['aktualny']
                                  else 'ZBLIZA_SIE_LUB_MINELA_SZACOWANA_STAWKA')
    return result


def funding_aktualny(hist, now_ms):
    """Zachowuje dokladnie dotychczasowe kryterium swiezosci cache."""
    return funding_cache_diagnostyka_v138(hist, now_ms)['aktualny']


# V6.139: stan diagnostyczny nie steruje decyzja o pobraniu danych.
FUNDING_PROGRESS_FILE_V139 = 'funding_progress_v139.json'


def funding_progress_v139(before, after, fetched, error='', previous_streak=0):
    """Sprawdza rzeczywisty postep serii historycznej po probie odswiezenia.

    Ta funkcja nie modyfikuje cache ani nie zmienia reguly swiezosci.
    """
    def normalized(hist):
        rows = {}
        if not isinstance(hist, list):
            return rows
        for item in hist:
            if not isinstance(item, dict):
                continue
            try:
                stamp = int(number(item.get('fundingTime')) or 0)
            except (ValueError, TypeError, OverflowError):
                continue
            if stamp > 0:
                rows[stamp] = str(item.get('fundingRate', ''))
        return rows
    old, new = normalized(before), normalized(after)
    old_max, new_max = max(old, default=0), max(new, default=0)
    if not fetched:
        status = 'CACHE_BEZ_ODSWIEZENIA'
    elif error:
        status = 'BLAD_API'
    elif not new:
        status = 'BRAK_POPRAWNEJ_HISTORII_Z_API'
    elif new_max > old_max:
        status = 'NOWSZA_STAWKA'
    elif new != old:
        status = 'ZMIANA_TRESCI_BEZ_NOWSZEJ_STAWKI'
    else:
        status = 'API_BEZ_POSTEPU'
    streak = previous_streak + 1 if status == 'API_BEZ_POSTEPU' else (previous_streak if not fetched else 0)
    return {'status_postepu': status, 'stare_ostatnie_ms': old_max or None,
            'nowe_ostatnie_ms': new_max or None, 'stare_liczba': len(old),
            'nowe_liczba': len(new), 'seria_identical': bool(old and new and old == new),
            'kolejne_odswiezenia_bez_postepu': streak,
            'ostrzezenie': ('POWTARZANY_BRAK_POSTEPU_API' if streak >= 2 else
                           'BRAK_POSTEPU_API' if status == 'API_BEZ_POSTEPU' else '')}


def test_funding_progress_v139():
    h = [{'fundingTime': str(1000*i), 'fundingRate': str(i/10000)} for i in range(1, 9)]
    same = funding_progress_v139(h, list(reversed(h)), True, previous_streak=1)
    assert same['status_postepu'] == 'API_BEZ_POSTEPU' and same['kolejne_odswiezenia_bez_postepu'] == 2
    assert same['ostrzezenie'] == 'POWTARZANY_BRAK_POSTEPU_API'
    newer = funding_progress_v139(h, h+[{'fundingTime':'9000','fundingRate':'0.01'}], True, previous_streak=2)
    assert newer['status_postepu'] == 'NOWSZA_STAWKA' and newer['kolejne_odswiezenia_bez_postepu'] == 0
    changed = [dict(x) for x in h]; changed[-1]['fundingRate'] = '0.009'
    assert funding_progress_v139(h, changed, True)['status_postepu'] == 'ZMIANA_TRESCI_BEZ_NOWSZEJ_STAWKI'
    assert funding_progress_v139(h, [], True, 'timeout')['status_postepu'] == 'BLAD_API'
    assert funding_progress_v139(h, h, False)['status_postepu'] == 'CACHE_BEZ_ODSWIEZENIA'
    return 'OK (powtarzane stale API, nowa stawka, korekta, blad, cache)'


def load_funding_progress_v139(drive_ok):
    if not drive_ok:
        return {}
    path = Path(DRIVE_DIR) / FUNDING_PROGRESS_FILE_V139
    try:
        data = json.loads(path.read_text(encoding='utf-8'))
        return data if isinstance(data, dict) else {}
    except (OSError, ValueError, TypeError):
        return {}


def save_funding_progress_v139(state, drive_ok):
    if not drive_ok:
        return 'BRAK_DRIVE'
    path = Path(DRIVE_DIR) / FUNDING_PROGRESS_FILE_V139
    temp = path.with_suffix(path.suffix + '.tmp')
    try:
        temp.write_text(json.dumps(state, ensure_ascii=False, sort_keys=True), encoding='utf-8')
        os.replace(temp, path)
        return 'OK'
    except (OSError, TypeError, ValueError) as exc:
        return 'BLAD: ' + str(exc)[:120]
    finally:
        try:
            temp.unlink(missing_ok=True)
        except OSError:
            pass


def test_funding_cache_v138():
    t = 1793260800000
    h = [{'fundingTime':str(t-(8-i)*8*3600000)} for i in range(8)]
    assert funding_aktualny(h, t-180000) is True
    assert funding_aktualny(h, t-60000) is False
    assert funding_cache_diagnostyka_v138(h, t-60000)['powod_szczegolowy'] == 'ZBLIZA_SIE_LUB_MINELA_SZACOWANA_STAWKA'
    assert funding_aktualny(None,t) is False
    assert funding_aktualny([{'fundingTime':'0'}],t) is False
    return 'OK (granica 2 min, brak historii, diagnostyka cyklu)'


def _fetch_count(key):
    with _api_lock:
        _fetch_stats[key] = _fetch_stats.get(key, 0) + 1


def fetch_coin(symbol, meta, ticker, now_ms, known=None, premium=None, fund=None):
    d = {'symbol': symbol, 'meta': meta, 'ticker': ticker, 'klines': {}, 'errors': {}}
    for tf, n in INTERVALS.items():
        try:
            known_tf = (known or {}).get(tf)
            n_get = need_bars(tf, n, known_tf, now_ms)
            reason = ('archiwum_aktualne' if n_get == 0 else
                      'naprawa_luki' if known_tf and len(known_tf) > 2 and not known_tf[2] else
                      'pelne_brak_potwierdzonej_historii' if n_get >= n else
                      'przyrost')
            _history_repairs[(symbol, tf)] = {'powod': reason, 'zadano': n_get,
                'ostatnia_archiwum_ms': (known_tf[1] if known_tf else None),
                'pierwsza_archiwum_ms': (known_tf[0] if known_tf else None),
                'archiwum_ciagle': (known_tf[2] if known_tf and len(known_tf)>2 else None)}
            if n_get == 0:
                _fetch_count(f'klines_{tf}_archiwum')
                d.setdefault('klines_przyrost', {})[tf] = 0      # V6.9: swiece z archiwum (main skleja)
                continue
            _fetch_count(f'klines_{tf}_' + ('przyrost' if n_get < n else 'pelne'))
            rows = fetch_klines(symbol, tf, n_get)
            if not rows:
                raise ValueError(f'Brak swiec {tf} z API (zadano {n_get}); archiwum do kontroli QC')
            if n_get < n:
                d.setdefault('klines_przyrost', {})[tf] = len(rows)
            _history_repairs[(symbol, tf)]['otrzymano'] = len(rows)
            # Potwierdzenie krotkiej historii wymaga pelnego zapytania.
            if n_get >= n and len(rows) < min(n, KLINES_MAX_LIMIT) and rows:
                _history_repairs[(symbol, tf)]['krotka_historia'] = True
                _history_repairs[(symbol, tf)]['pierwsza_swieca'] = int(number(rows[0][0]))
                _short_history_verified[(symbol, tf)] = int(number(rows[0][0]))
            t_now = int(time.time() * 1000) + _clock['offset_ms']   # swieca mogla sie zamknac w trakcie pobierania
            d['klines'][tf] = {'data': rows, 'last_closed': bool(rows) and bar_closed(rows[-1], tf, t_now)}
        except Exception as exc:
            _history_repairs.setdefault((symbol, tf), {})['blad'] = str(exc)[:200]
            d['errors'][f'klines_{tf}'] = str(exc)[:200]
            if '-1142' in str(exc) or "is invalid" in str(exc):   # V6.30: WEEX odrzuca symbol - nic wiecej nie pobieraj
                d['errors']['symbol_niewazny'] = True
                return d
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
        _fetch_count('funding_z_archiwum')
    else:
        _fetch_count('funding_odswiezenie')
    for key, (path, params) in calls.items():
        try:
            d[key] = unwrap(weex(path, params))
            if key == 'open_interest':
                _io_stat('oi_sukces')
        except ValueError as exc:
            if key == 'funding_history' and FUNDING_LIMIT != FUNDING_LIMIT_FALLBACK:
                try:
                    d[key] = unwrap(weex(path, {**params, 'limit': FUNDING_LIMIT_FALLBACK}))
                    continue
                except Exception as exc2:
                    exc = exc2
            d['errors'][key] = str(exc)[:200]
            if key == 'open_interest':
                _io_stat('oi_blad')
        except Exception as exc:
            d['errors'][key] = str(exc)[:200]
            if key == 'open_interest':
                _io_stat('oi_blad')
    for lim in DEPTH_LIMITS:
        try:
            d['depth'] = unwrap(weex('/capi/v3/market/depth', {'symbol': symbol, 'limit': lim}))
            _io_stat('depth_200_sukces' if lim == 200 else 'depth_15_awaryjny')
            break
        except Exception as exc:
            d['errors']['depth'] = str(exc)[:200]
    if 'depth' not in d:
        _io_stat('depth_blad')
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


def universe_rows(meta, tick, usdt, chosen, is_tf):
    """V6.62: kazdy kontrakt z exchangeInfo + rank wolumenu (osobno krypto / TradFi) i powod pominiecia."""
    rk = {}
    for grp in ([x for x in usdt if not is_tf(x)], [x for x in usdt if is_tf(x)]):
        rk.update({s_: i for i, s_ in enumerate(grp, 1)})
    wybrane = set(chosen)
    rows = []
    for s_ in usdt:
        qv_ = number(tick[s_].get('quoteVolume'))
        tf_ = is_tf(s_)
        if s_ in wybrane:
            why = ''
        elif qv_ < MIN_QUOTE_VOL_USDT:
            why = f'wolumen 24h < {MIN_QUOTE_VOL_USDT / 1e6:.1f}M'
        elif tf_ and not TRADFI_ON:
            why = 'TradFi wylaczone (TRADFI_ON)'
        else:
            why = f"poza top {TRADFI_N if tf_ else TOP_N} ({'TradFi' if tf_ else 'krypto'})"
        rows.append({'symbol': s_, 'rodzaj': 'TradFi' if tf_ else 'krypto', 'rank_wolumenu': rk.get(s_),
                     'quote_vol_24h': round(qv_), 'chg_24h_pct': round(number(tick[s_].get('priceChangePercent')) * 100, 2),
                     'pobrany': s_ in wybrane, 'powod_pominiecia': why})
    w_usdt = set(usdt)
    for s_, m_ in meta.items():
        if s_ in w_usdt:
            continue
        if s_ not in tick:
            why = 'brak tickera 24h'
        else:
            why = f"nie USDT perpetual ({m_.get('quoteAsset')}, {m_.get('contractType')})"
        rows.append({'symbol': s_, 'rodzaj': None, 'rank_wolumenu': None,
                     'quote_vol_24h': round(number((tick.get(s_) or {}).get('quoteVolume'))), 'chg_24h_pct': None,
                     'pobrany': False, 'powod_pominiecia': why})
    return rows


def collect(known=None, fund_cache=None):
    now_ms = server_ms()                   # V6.15: ustawia tez _clock['offset_ms'] (polowa RTT)
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
    if TRADFI_ON and WATCH_TRADFI:      # V6.34: lewarowane ETF-y z listy obserwowanych
        jest = [s_ for s_ in usdt if s_.endswith('USDT') and s_[:-4] in WATCH_TRADFI and is_tf(s_)]
        dod = [s_ for s_ in jest if s_ not in chosen][:WATCH_TRADFI_MAX]
        chosen += dod
        brak = [b for b in WATCH_TRADFI if b + 'USDT' not in jest]
        print(f'  lista obserwowanych TradFi: na WEEX {len(jest)}/{len(WATCH_TRADFI)} '
              f'({", ".join(x[:-4] for x in jest) or "-"}), dodane ponad top: {", ".join(x[:-4] for x in dod) or "-"}'
              + (f'; brak na WEEX: {len(brak)}' if brak else ''))
    brak_ai = [x for x in dict.fromkeys(ALWAYS_INCLUDE) if x not in tick]   # V6.62: wczesniej pomijane bez sladu
    if brak_ai:
        print(f'  ALWAYS_INCLUDE - brak na WEEX (albo brak tickera): {", ".join(brak_ai)}')
    try:                                   # V6.62: universe.csv - dlaczego kontrakt nie trafil do analizy
        OUT.mkdir(parents=True, exist_ok=True)
        pd.DataFrame(universe_rows(meta, tick, usdt, chosen, is_tf)).to_csv(OUT / 'universe.csv', index=False)
    except Exception as exc:
        print(f'  universe.csv: blad ({str(exc)[:100]})')
    n_tf =sum(1 for s_ in top if is_tf(s_))
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
    retry = [s for s, d in coins.items() if 'klines_5m' in (d.get('errors') or {}) and not (d.get('errors') or {}).get('symbol_niewazny')] + [s for s in chosen if s not in coins]
    if retry:                              # V6.10: drugie podejscie dla kontraktow z bledem swiec 5m
        print(f'  ponowne pobieranie ({len(retry)}): {", ".join(retry[:10])}{" ..." if len(retry) > 10 else ""}')
        # Nie pobieramy ponownie 4 TF, funding, OI i ksiazki, jesli
        # nie udaly sie tylko swiece 5m. Inne dane pozostaja nienaruszone.
        missing = [sym for sym in retry if sym not in coins]
        partial = [sym for sym in retry if sym in coins]
        def retry_5m(sym):
            count = need_bars('5m', INTERVALS['5m'], (known.get(sym) or {}).get('5m'), now_ms)
            if count == 0:
                return sym, None, 0
            rows = fetch_klines(sym, '5m', count)
            return sym, rows, count
        with ThreadPoolExecutor(max_workers=WORKERS) as ex:
            jobs = {ex.submit(retry_5m, sym): ('partial', sym) for sym in partial}
            jobs.update({ex.submit(fetch_coin, sym, meta[sym], tick[sym], now_ms,
                                             known.get(sym), prem.get(sym), fund_cache.get(sym)):
                         ('missing', sym) for sym in missing})
            for fut in as_completed(jobs):
                kind, sym = jobs[fut]
                try:
                    if kind == 'missing':
                        d2 = fut.result()
                        coins[sym] = d2
                    else:
                        _, rows, count = fut.result()
                        if count == 0:
                            coins[sym].setdefault('klines_przyrost', {})['5m'] = 0
                        else:
                            t_now = int(time.time() * 1000) + _clock['offset_ms']
                            coins[sym]['klines']['5m'] = {
                                'data': rows,
                                'last_closed': bool(rows) and bar_closed(rows[-1], '5m', t_now),
                            }
                            if count < INTERVALS['5m']:
                                coins[sym].setdefault('klines_przyrost', {})['5m'] = len(rows)
                        coins[sym]['errors'].pop('klines_5m', None)
                except Exception as exc:
                    print(f'  {sym}: blad ponownie ({str(exc)[:80]})')
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
    arr, forming, bad = {}, {}, {}
    reject_details = {}
    for tf in ('5m', '15m', '1h', '4h'):
        k = d.get('klines', {}).get(tf)
        if not k or not k.get('data'):
            continue
        a = rows_array(k['data'])
        a = a[np.argsort(a[:, T], kind='stable')]
        keep = np.append(np.diff(a[:, T]) > 0, True)  # bez duplikatow czasu
        # V6.115: diagnozuj DOKLADNIE te rekordy, ktore usuwala stara logika.
        # Nie dodawaj tutaj nowych warunkow odrzucania: raport jest tylko informacyjny.
        reasons = {}
        examples = []
        def _reject_sample(row, reason):
            reasons[reason] = reasons.get(reason, 0) + 1
            if len(examples) < 5:
                def safe_num(i):
                    return float(row[i]) if np.isfinite(row[i]) else None
                examples.append({'powod': reason, 'open_ms': int(row[T]) if np.isfinite(row[T]) else None,
                                 'open': safe_num(O), 'high': safe_num(H),
                                 'low': safe_num(L), 'close': safe_num(C),
                                 'volume': safe_num(V)})
        for row in a[~keep]:
            _reject_sample(row, 'DUPLIKAT_ZNACZNIKA_CZASU')
        a = a[keep]
        ok = ((a[:, L] > 0) & (a[:, H] >= np.maximum(a[:, O], a[:, C])) & (a[:, L] <= np.minimum(a[:, O], a[:, C]))
              & np.isfinite(a[:, :5]).all(axis=1))
        if not ok.all():
            for row in a[~ok]:
                if not np.isfinite(row[:5]).all():
                    reason = 'NIEKONCZONA_WARTOSC_OHLCV'
                elif row[L] <= 0:
                    reason = 'LOW_NIEDODATNIE'
                elif row[H] < max(row[O], row[C]):
                    reason = 'HIGH_PONIZEJ_OPEN_LUB_CLOSE'
                else:
                    reason = 'LOW_POWYZEJ_OPEN_LUB_CLOSE'
                _reject_sample(row, reason)
            bad[tf] = int((~ok).sum())
            a = a[ok]
        if reasons:
            reject_details[tf] = {'powody': reasons, 'przyklady': examples,
                                   'odrzucone_ohlc': bad.get(tf, 0),
                                   'duplikaty': int((~keep).sum())}
        if k.get('last_closed') is False and len(a) > 1:
            forming[tf], a = a[-1], a[:-1]
        arr[tf] = a
    meta = d.get('meta') or {}
    taker = number(meta.get('takerFeeRate'), TAKER_FEE_BPS / 1e4) * 1e4 or TAKER_FEE_BPS
    maker = number(meta.get('makerFeeRate'), MAKER_FEE_BPS / 1e4) * 1e4 or MAKER_FEE_BPS
    return {'arr': arr, 'forming': forming, 'fees': (taker, maker),
            'asset_type': meta.get('underlyingType') or 'COIN', 'raw': d, 'zle_swiece': bad,
            'diagnostyka_odrzucen': reject_details,
            'tick_dec': tick_decimals(meta)}


def tick_decimals(meta):
    """V6.15: liczba miejsc po przecinku ceny kontraktu (pricePrecision z exchangeInfo) albo None."""
    v = (meta or {}).get('pricePrecision')
    try:
        v = int(v)
    except (TypeError, ValueError):
        return None
    return v if 0 <= v <= 12 else None


def round_tick(x, dec, mode='nearest'):
    """V6.15: cena do ticka 10^-dec. mode: nearest / down / up."""
    if x is None or dec is None or not math.isfinite(x):
        return x
    q = Decimal(1).scaleb(-dec)
    rm = {'nearest': ROUND_HALF_UP, 'down': ROUND_FLOOR, 'up': ROUND_CEILING}[mode]
    return float(Decimal(repr(float(x))).quantize(q, rounding=rm))


def ceny_do_ticka(r, dec):
    """V6.15: ceny setupu do ticka: entry/zone/1R do najblizszego, SL na zewnatrz, TP w strone wejscia."""
    if not CENY_DO_TICKA or dec is None:
        return
    out = 'down' if r['side'] == 'LONG' else 'up'      # LONG: SL nizej, TP nizej (blizej wejscia); SHORT odwrotnie
    inn = out
    for k in ('entry', 'tp_1R'):                       # V6.20: zone_ref bez zaokraglenia (poziom odniesienia)
        if r.get(k) is not None:
            r[k] = round_tick(r[k], dec)
    if r.get('sl') is not None:
        r['sl'] = round_tick(r['sl'], dec, out)
    for k in ('tp1', 'tp2', 'tp_liq'):
        if r.get(k) is not None:
            r[k] = round_tick(r[k], dec, inn)
    r['tick'] = 10 ** -dec


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


_FUND_CACHE = {}


def funding_z_at(raw, ts):
    """Funding z w chwili ts tylko z rozliczonych stawek do ts: (ostatnia - srednia) / odch. std (min. 8 stawek)."""
    hist = (raw or {}).get('funding_history')
    if not isinstance(hist, list):
        return None
    # V6.97 (szybkosc): historia sortowana raz na kontrakt (cache), potem tylko wyciecie do ts (bisect) -
    # wczesniej sortowanie calej listy przy kazdym zdjeciu testu wstecznego. Ten sam wynik.
    c = _FUND_CACHE.get(id(hist))
    if c is None or c[0] is not hist or c[1] != len(hist):
        allp = sorted((int(number(x.get('fundingTime'))), number(x.get('fundingRate'))) for x in hist
                      if isinstance(x, dict) and number(x.get('fundingTime')))
        c = (hist, len(hist), allp, [t for t, _ in allp])
        _FUND_CACHE[id(hist)] = c
    pts = c[2][:bisect.bisect_right(c[3], ts)]
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


def setup_odbicie(s, A, f, price):
    """V6.53: LONG po duzym spadku: cena >= ODBICIE_SPADEK_PCT nizej niz 4h temu -> wejscie po rynku,
    SL/TP staly w ATR 1h. Klucz strefy = najwyzsza cena z 4h (ten sam spadek = ta sama okazja)."""
    if s != 1:
        return None
    a5 = A['5m']
    if len(a5) < ODBICIE_KLUCZ_H * 12 + 1 or not f.get('atr1'):
        return None
    chg = (price / a5[-48, O] - 1) * 100
    if chg > -ODBICIE_SPADEK_PCT:
        return None
    szczyt = float(a5[-ODBICIE_KLUCZ_H * 12:, H].max())   # szczyt 12h = klucz: dalsze zjazdy tego samego krachu to ta sama okazja
    c1 = A['1h'][:, C]
    przed = (c1[-5] / c1[-29] - 1) * 100 if len(c1) >= 29 and c1[-29] > 0 else None   # V6.54: 24h przed spadkiem
    atr1 = f['atr1']
    n_ = ODBICIE_KLUCZ_H * 12                          # V6.62: cena przy pierwszym sygnale tej okazji (tylko informacja)
    pierwsza = None
    if len(a5) >= n_ + 48:
        ref, cl = a5[-n_ - 48:-48, O], a5[-n_:, C]
        spad = np.where(ref > 0, cl / np.where(ref > 0, ref, 1.0) - 1, 0.0) * 100
        hit = np.nonzero(spad <= -ODBICIE_SPADEK_PCT)[0]
        if len(hit):
            pierwsza = float(cl[hit[0]])
    gonienie = (price - pierwsza) / atr1 if pierwsza else None
    trig = f"spadek {chg:.1f}% w 4h (szczyt {psig(szczyt)}) -> LONG po rynku, SL {ODBICIE_SL_ATR:g} ATR, TP {ODBICIE_TP_ATR:g} ATR 1h"
    if pierwsza:
        trig += f" | pierwszy sygnal {psig(pierwsza)} (teraz {gonienie:+.2f} ATR)"
    return {'entry': szczyt, 'sl': price - ODBICIE_SL_ATR * atr1, 'tp_fixed': price + ODBICIE_TP_ATR * atr1,
            'sl_fixed': True, 'trigger': trig, 'sl_basis': f'{ODBICIE_SL_ATR:g} ATR 1h', 'zone': (price, price),
            'targets': ([], []), 'extra': {'spadek_4h_pct': chg, 'wzrost_przed_24h_pct': przed,
                                           'odbicie_pierwsza_cena': pierwsza, 'odbicie_gonienie_atr': gonienie}}


def zarz_1r(typ):
    """V6.53: czy typ zamyka czesc pozycji na +1R."""
    return ZARZADZANIE_1R and typ not in BEZ_1R_TYPY


def in_market_zone(s, raw, price, atr1):
    """Cena w strefie wejscia albo najwyzej MARKET_ZONE_ATR przed nia (od strony, z ktorej nadchodzi)."""
    lo, hi = raw['zone']
    if s == 1:
        return lo <= price <= hi + MARKET_ZONE_ATR * atr1
    return lo - MARKET_ZONE_ATR * atr1 <= price <= hi


_REJ = {'on': False, 'sym': None, 'rows': []}     # V6.15: odrzucone przy budowie setupu (tylko na zywo)


def _odrzuc(typ, s, mode, why):
    """V6.15: zapis setupu wykrytego, ale odrzuconego w finalize (odrzucone.csv). Zwraca None."""
    if _REJ['on']:
        _REJ['rows'].append({'symbol': _REJ['sym'], 'setup_type': typ, 'side': 'LONG' if s == 1 else 'SHORT',
                             'entry_type': mode, 'powod': why})
    return None


def finalize(typ, s, raw, f, price, fees, targets, mode='LIMIT'):
    """Ryzyko, cele, R:R netto, koszt w R i filtry. None = setup niepoprawny konstrukcyjnie.
    mode='LIMIT': zlecenie na raw['entry'] (cena musi byc jeszcze przed nim); 'MARKET': wejscie po cenie teraz.
    SWING4H: ograniczenia (min/max SL, odleglosc, TP) w ATR 4h zamiast 1h."""
    atr1 = f['atr4'] if typ == 'SWING4H' and f.get('atr4') else f['atr1']
    market = mode == 'MARKET'
    entry, sl = (price if market else raw['entry']), raw['sl']
    if not market and s * (price - entry) <= 0:
        return _odrzuc(typ, s, mode, 'cena juz na/za wejsciem')   # to nie jest LIMIT
    if s * (entry - sl) <= 0:
        return _odrzuc(typ, s, mode, 'SL po zlej stronie wejscia')
    if SL_ZA_KLASTER and not raw.get('sl_fixed'):
        against0 = (f.get('liq', ([], []))[1] if s == 1 else f.get('liq', ([], []))[0])
        near0 = [cp for cp, _ in against0 if 0 <= s * (sl - cp) <= 0.5 * atr1]
        if near0:
            far = min(near0) if s == 1 else max(near0)
            sl = far - s * SL_KLASTER_BUFOR_ATR * atr1
    if abs(entry - sl) < SETUP_MIN_SL_ATR * atr1:
        sl = entry - s * SETUP_MIN_SL_ATR * atr1
    risk = abs(entry - sl)
    max_risk_pct = ODBICIE_MAX_RISK_PCT if typ == 'ODBICIE' else SETUP_MAX_RISK_PCT   # V6.53
    if risk > SETUP_MAX_SL_ATR * atr1 or risk / entry * 100 > max_risk_pct:
        return _odrzuc(typ, s, mode, f'SL za szeroki ({risk / atr1:.1f} ATR, {risk / entry * 100:.1f}%)')
    if abs(price - entry) > LIMIT_MAX_DIST_ATR * atr1:
        return _odrzuc(typ, s, mode, f'LIMIT za daleko ({abs(price - entry) / atr1:.1f} ATR)')
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
    if raw.get('tp_fixed') is not None:                 # V6.53: staly TP (ODBICIE)
        tp1, basis = raw['tp_fixed'], f'{ODBICIE_TP_ATR:g} ATR'
        tps = []
    for x in tps:
        if abs(x - entry) > TP_MAX_R * risk:
            break
        if rr_net(x) >= SETUP_MIN_RR:
            tp1, basis = x, 'plynnosc'
            break
    if tp1 is None:
        tp1, basis = entry + s * TP_DEFAULT_R * risk, f'{TP_DEFAULT_R}R'
    if rr_net(tp1) < SETUP_MIN_RR:
        return _odrzuc(typ, s, mode, f'RR netto {rr_net(tp1):.2f} < {SETUP_MIN_RR}')
    if s * (tp1 - price) <= 0:
        return _odrzuc(typ, s, mode, 'cena juz za TP1')
    if not market and s * (price - entry) >= LIMIT_MAX_PROGRESS * abs(tp1 - entry):
        return _odrzuc(typ, s, mode, 'cena przeszla polowe drogi do TP1')
    further = [x for x in tps if s * (x - tp1) > 0.5 * risk]
    tp2 = further[0] if further and abs(further[0] - entry) <= 6 * risk else tp1 + s * risk
    cost_r = cost_in_sl / risk
    ext_entry = s * (entry - f['e50_1']) / atr1
    filters = []
    if cost_r > MAX_COST_R:
        filters.append('koszt')
    if FILTR_POSCIG and ext_entry > EXT_CHASE_MAX_ATR:
        filters.append('poscig')
    if typ_wylaczony(typ, 'LONG' if s == 1 else 'SHORT', mode):   # V6.71: tez WYLACZONE_WEJSCIA
        filters.append('typ_wylaczony')
    if s == -1 and f['ext_ema50_atr_1h'] > SHORT_SQUEEZE_EXT_ATR:
        filters.append('squeeze')
    wp = (raw.get('extra') or {}).get('wzrost_przed_24h_pct')
    if typ == 'ODBICIE' and ODBICIE_PO_POMPIE_PCT is not None and wp is not None and wp > ODBICIE_PO_POMPIE_PCT:   # V6.54
        filters.append('po_pompie')
    if f['vol_anomaly'] and typ != 'ODBICIE':          # V6.53: po krachu wolumen zawsze nietypowy
        filters.append('anomalia_wolumenu')
    if FILTR_AZJA_SHORT and s == -1 and f.get('session') == 'azja':
        filters.append('azja_short')
    if FILTR_SHORT_GODZINY_UTC and s == -1 and f.get('hour_utc') in FILTR_SHORT_GODZINY_UTC:
        filters.append('short_godziny')
    if SWING4H_BEZ_FVG4H and typ == 'SWING4H' and (raw.get('extra') or {}).get('fvg_tf') == '4h':
        filters.append('fvg4h')
    if FILTER_COUNTER_TREND and typ not in ('CROWD', 'ODBICIE') and (f.get('trend_4h') == -s or (f.get('btc_trend_1h') == -s and (f.get('corr_btc') or 0) >= BTC_CORR_MIN)):
        filters.append('przeciw_trendowi')
    row = {'setup_type': typ, 'typ_wersja': f"{typ}.v{SETUP_TYPES[typ]}{'.M' if market else ''}",
           'side': 'LONG' if s == 1 else 'SHORT', 'entry_type': mode, 'zone_ref': psig(raw['entry'], 7), 'entry': psig(entry, 7), 'sl': psig(sl, 7), 'tp1': psig(tp1, 7), 'tp2': psig(tp2, 7),
           'rr_tp1_net': rnd(rr_net(tp1), 2), 'risk_pct': rnd(risk / entry * 100, 3), 'sl_atr': rnd(risk / f['atr1'], 2),
           'tp_1R': psig(entry + s * risk, 7) if zarz_1r(typ) else None,
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
    step = 10 ** -qty_prec
    min_qty = max(step, number(meta.get('minOrderSize'), 0) or 0)
    extra = {'ilosc_za_mala': False, 'ilosc_min': None, 'ryzyko_min_usdt': None}
    if qty <= 0 or qty < min_qty:                       # V6.32: najmniejsza mozliwa pozycja i jej realne ryzyko
        r_min = min_qty * entry * (sl_frac + cost_frac)
        extra = {'ilosc_za_mala': True, 'ilosc_min': min_qty, 'ryzyko_min_usdt': rnd(r_min, 2)}
        if r_min <= ILOSC_MIN_MAX_RYZYKO * risk_usdt:      # V6.61: pokaz minimum zamiast 0.0
            qty = min_qty
        uwagi.append(f'ilosc ponizej minimum kontraktu: min. {min_qty:g} = ryzyko {r_min:.2f} USDT (zalozone {risk_usdt:.2f})')
    return {**extra,'dzwignia_pref': int(PREFEROWANA_DZWIGNIA), 'dzwignia_bezp': lev_safe, 'dzwignia': lev,
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


# ============================== ELLIOTT - BADANIE, BEZ ZMIAN SETUPOW ==============================
ELLIOTT_COLS = ['elliott_faza', 'elliott_kierunek', 'elliott_uniewaznienie',
                'elliott_pivoty_1h', 'elliott_pivoty_4h', 'elliott_trend_4h',
                'elliott_zgodnosc_1h_4h', 'elliott_uwagi',
                'elliott_fala2_proc', 'elliott_fala4_proc', 'elliott_fala3_do_1', 'elliott_fala5_do_1',
                'elliott_do_uniewaznienia_atr',
                'elliott_abc_status', 'elliott_abc_kierunek', 'elliott_abc_a', 'elliott_abc_b', 'elliott_abc_c',
                'elliott_abc_fib', 'elliott_abc_uniewaznienie', 'elliott_abc_zgodnosc_4h',
                'elliott_abc_odleglosc_atr', 'elliott_abc_fib382', 'elliott_abc_fib500', 'elliott_abc_fib618',
                'elliott_abc_c_utc_ms', 'elliott_abc_wiek_h', 'elliott_abc_od_c_atr', 'elliott_abc_swiezosc']   # V6.157: przeplyw metadanych C
# V6.103: proporcje fal (Fibonacci) i odleglosc od uniewaznienia
ELLIOTT_LEWY = 3
ELLIOTT_PRAWY = 3
ELLIOTT_MIN_ATR = 0.8


def elliott_pivoty(arr, left=ELLIOTT_LEWY, right=ELLIOTT_PRAWY, min_atr=ELLIOTT_MIN_ATR):
    """Pivoty ze znanych swiec; prog ruchu ma ATR z chwili POTWIERDZENIA nowego pivota.

    Zwraca (czas ekstremum, typ, cena). Kolumny wejscia musza zawierac
    wylacznie zamkniete swiece dostepne w chwili zdjecia rynku.
    """
    if arr is None or len(arr) < max(35, left + right + 5):
        return []
    hh, ll, cc = arr[:, H], arr[:, L], arr[:, C]
    atr_hist = atr_series(hh, ll, cc)
    k = left + right + 1
    win_h = np.lib.stride_tricks.sliding_window_view(hh, k)
    win_l = np.lib.stride_tricks.sliding_window_view(ll, k)
    srod_h, srod_l = win_h[:, left], win_l[:, left]
    is_high = (win_h.argmax(axis=1) == left) & ((win_h == srod_h[:, None]).sum(axis=1) == 1)
    is_low = (win_l.argmin(axis=1) == left) & ((win_l == srod_l[:, None]).sum(axis=1) == 1)
    out = []
    for j in np.nonzero(is_high != is_low)[0]:
        i = int(j) + left
        confirm_i = i + right
        threshold_atr = float(atr_hist[confirm_i])
        if not np.isfinite(threshold_atr) or threshold_atr <= 0:
            continue
        typ = 'H' if is_high[j] else 'L'
        pivot = (int(arr[i, T]), typ, float(hh[i] if typ == 'H' else ll[i]))
        if not out:
            out.append(pivot)
        elif typ == out[-1][1]:
            if (typ == 'H' and pivot[2] > out[-1][2]) or (typ == 'L' and pivot[2] < out[-1][2]):
                out[-1] = pivot
        elif abs(pivot[2] - out[-1][2]) >= min_atr * threshold_atr:
            out.append(pivot)
    return out


def elliott_trend_pivotow(p):
    """Trend 4h tylko gdy potwierdzono dwa ostatnie szczyty i dwa dolki."""
    highs = [v for _, typ, v in p if typ == 'H']
    lows = [v for _, typ, v in p if typ == 'L']
    if len(highs) < 2 or len(lows) < 2:
        return 'NIEJEDNOZNACZNE'
    if highs[-1] > highs[-2] and lows[-1] > lows[-2]:
        return 'LONG'
    if highs[-1] < highs[-2] and lows[-1] < lows[-2]:
        return 'SHORT'
    return 'NIEJEDNOZNACZNE'


def elliott_abc_badanie(p1, a1, trend_4h):
    """ABC zigzag research z WYŁĄCZNIE potwierdzonych pivotów.

    Wymaga czterech naprzemiennych punktów H-L-H-L (korekta spadkowa
    po wzroście) lub L-H-L-H (korekta wzrostowa po spadku), oraz
    poprzedniego pivota, aby oszacować zniesienie ruchu wcześniejszego.
    To hipoteza, nie automatyczny sygnał kupna/sprzedaży.
    """
    out = dict.fromkeys(['elliott_abc_status', 'elliott_abc_kierunek', 'elliott_abc_a',
        'elliott_abc_b', 'elliott_abc_c', 'elliott_abc_fib', 'elliott_abc_uniewaznienie',
        'elliott_abc_zgodnosc_4h', 'elliott_abc_odleglosc_atr',
        'elliott_abc_fib382', 'elliott_abc_fib500', 'elliott_abc_fib618',
        'elliott_abc_c_utc_ms', 'elliott_abc_wiek_h', 'elliott_abc_od_c_atr', 'elliott_abc_swiezosc'])
    out['elliott_abc_status'] = 'BRAK_POTWIERDZENIA'
    if len(p1) < 5 or a1 is None or len(a1) < 20:
        return out
    q = p1[-5:]
    types = [x[1] for x in q]
    v = [float(x[2]) for x in q]
    if types == ['L', 'H', 'L', 'H', 'L']:
        # Poprzedni impuls w górę: L0->H1; korekta A=L2 B=H3 C=L4.
        side = 'LONG'
        valid = v[1] > v[0] and v[2] < v[1] and v[3] < v[1] and v[3] > v[2] and v[4] <= v[2] and v[4] > v[0]
        impulse = v[1] - v[0]
        retr = (v[1] - v[4]) / impulse if impulse > 0 else None
        fib = lambda k: v[1] - k * impulse
        invalidation = v[0]
    elif types == ['H', 'L', 'H', 'L', 'H']:
        side = 'SHORT'
        valid = v[1] < v[0] and v[2] > v[1] and v[3] > v[1] and v[3] < v[2] and v[4] >= v[2] and v[4] < v[0]
        impulse = v[0] - v[1]
        retr = (v[4] - v[1]) / impulse if impulse > 0 else None
        fib = lambda k: v[1] + k * impulse
        invalidation = v[0]
    else:
        return out
    if not valid or retr is None or not (0 < retr < 1):
        return out
    last = float(a1[-1, C])
    # Potwierdzenie pivota C NIE oznacza potwierdzenia odwrócenia rynku.
    if (side == 'LONG' and last <= invalidation) or (side == 'SHORT' and last >= invalidation):
        return out
    a_ = atr(a1[:, H], a1[:, L], a1[:, C])
    direction = 1 if side == 'LONG' else -1
    # Wiek liczony od czasu potwierdzonego pivota C (bez wykorzystania przyszlych swiec).
    c_ms = int(q[-1][0])
    age_h = max(0., (float(a1[-1, T]) - c_ms) / HOUR_MS)
    c_dist_atr = abs(last - v[4]) / a_ if a_ and np.isfinite(a_) and a_ > 0 else None
    freshness = ('BRAK_DANYCH' if c_dist_atr is None else
                 'SWIEZY' if age_h <= 12 and c_dist_atr <= 1.5 else
                 'ODDALONY' if c_dist_atr > 2.5 else 'STARSZY')
    out.update(elliott_abc_c_utc_ms=c_ms, elliott_abc_wiek_h=rnd(age_h, 2),
               elliott_abc_od_c_atr=rnd(c_dist_atr, 2) if c_dist_atr is not None else None,
               elliott_abc_swiezosc=freshness)
    out.update(elliott_abc_status='ABC_POTENCJALNIE_ZAKONCZONE', elliott_abc_kierunek=side,
               elliott_abc_a=rnd(v[2], 8), elliott_abc_b=rnd(v[3], 8), elliott_abc_c=rnd(v[4], 8),
               elliott_abc_fib=rnd(retr, 3), elliott_abc_uniewaznienie=rnd(invalidation, 8),
               elliott_abc_zgodnosc_4h=('ZGODNE' if trend_4h == side else 'PRZECIWNE' if trend_4h in ('LONG','SHORT') else 'NIEJEDNOZNACZNE'),
               elliott_abc_odleglosc_atr=rnd(direction * (last - invalidation)/a_, 2) if a_ and np.isfinite(a_) and a_ > 0 else None,
               elliott_abc_fib382=rnd(fib(.382), 8), elliott_abc_fib500=rnd(fib(.5), 8),
               elliott_abc_fib618=rnd(fib(.618), 8))
    return out


def elliott_struktura(A):
    """Heurystyka podobienstwa fal 1-5 na 1h; NIE prognoza ani sygnal handlowy.

    IMPULS_ROZWIJA_SIE: potwierdzone pivoty do potencjalnej fali 4,
    IMPULS_5_POTWIERDZONY: potwierdzony ekstremum potencjalnej fali 5.
    ABC nie jest rozpoznawane. 4h opisuje odrebny trend swingowy.
    """
    result = dict.fromkeys(ELLIOTT_COLS)
    result.update(elliott_faza='NIEJEDNOZNACZNE', elliott_kierunek='BRAK',
                  elliott_trend_4h='NIEJEDNOZNACZNE',
                  elliott_zgodnosc_1h_4h='BRAK',
                  elliott_uwagi='za malo potwierdzonych pivotow')
    p1 = elliott_pivoty(A.get('1h'))
    p4 = elliott_pivoty(A.get('4h'))
    result['elliott_pivoty_1h'] = len(p1)
    result['elliott_pivoty_4h'] = len(p4)
    result['elliott_trend_4h'] = elliott_trend_pivotow(p4)
    result.update(elliott_abc_badanie(p1, A.get('1h'), result['elliott_trend_4h']))
    if len(p1) < 5:
        return result

    def rozwija(pp):
        pr = [x[2] for x in pp]
        ty = [x[1] for x in pp]
        if ty == ['L','H','L','H','L'] and pr[2] > pr[0] and pr[3] > pr[1] and pr[4] > pr[1]:
            return 'LONG', pr[4]
        if ty == ['H','L','H','L','H'] and pr[2] < pr[0] and pr[3] < pr[1] and pr[4] < pr[1]:
            return 'SHORT', pr[4]
        return None, None

    if len(p1) >= 6:
        pp = p1[-6:]
        direction, invalidation = rozwija(pp[:5])
        pr = [x[2] for x in pp]
        if direction == 'LONG' and pp[-1][1] == 'H' and pr[5] > pr[3]:
            lengths = [pr[1]-pr[0], pr[3]-pr[2], pr[5]-pr[4]]
            if lengths[1] > min(lengths[0], lengths[2]):
                result.update(elliott_faza='IMPULS_5_POTWIERDZONY', elliott_kierunek='LONG',
                              elliott_uniewaznienie=invalidation,
                              elliott_uwagi='heurystyka 1h; piate ekstremum potwierdzone')
        elif direction == 'SHORT' and pp[-1][1] == 'L' and pr[5] < pr[3]:
            lengths = [pr[0]-pr[1], pr[2]-pr[3], pr[4]-pr[5]]
            if lengths[1] > min(lengths[0], lengths[2]):
                result.update(elliott_faza='IMPULS_5_POTWIERDZONY', elliott_kierunek='SHORT',
                              elliott_uniewaznienie=invalidation,
                              elliott_uwagi='heurystyka 1h; piate ekstremum potwierdzone')
    if result['elliott_faza'] == 'NIEJEDNOZNACZNE':
        direction, invalidation = rozwija(p1[-5:])
        if direction:
            result.update(elliott_faza='IMPULS_ROZWIJA_SIE', elliott_kierunek=direction,
                          elliott_uniewaznienie=invalidation,
                          elliott_uwagi='hipoteza 1h: do fali 4, bez potwierdzenia fali 5')
    if result['elliott_kierunek'] in ('LONG', 'SHORT'):
        trend = result['elliott_trend_4h']
        result['elliott_zgodnosc_1h_4h'] = ('ZGODNE' if trend == result['elliott_kierunek']
                                            else 'PRZECIWNE' if trend in ('LONG', 'SHORT')
                                            else 'NIEJEDNOZNACZNE')
        result.update(elliott_proporcje(p1, result['elliott_faza'], result['elliott_kierunek'],
                                        result['elliott_uniewaznienie'], A.get('1h')))   # V6.75
    return result


def elliott_proporcje(p1, faza, kierunek, uniewaznienie, a1):
    """V6.75: liczby opisujace rozpoznane fale (tylko informacja): cofniecie fali 2 i 4 (% fali 1 i 3), fala 3 i 5
    jako krotnosc fali 1, odleglosc zamkniecia ostatniej swiecy 1h od poziomu uniewaznienia w ATR 1h
    (dodatnia = cena po stronie impulsu)."""
    out = dict.fromkeys(['elliott_fala2_proc', 'elliott_fala4_proc', 'elliott_fala3_do_1', 'elliott_fala5_do_1',
                         'elliott_do_uniewaznienia_atr'])
    s = 1 if kierunek == 'LONG' else -1
    n = 6 if faza == 'IMPULS_5_POTWIERDZONY' else 5
    if len(p1) < n:
        return out
    pr = [x[2] for x in p1[-n:]]
    w1, w3 = s * (pr[1] - pr[0]), s * (pr[3] - pr[2])
    if w1 <= 0 or w3 <= 0:
        return out
    out['elliott_fala2_proc'] = rnd(s * (pr[1] - pr[2]) / w1 * 100, 1)
    out['elliott_fala3_do_1'] = rnd(w3 / w1, 3)
    out['elliott_fala4_proc'] = rnd(s * (pr[3] - pr[4]) / w3 * 100, 1)
    if n == 6:
        out['elliott_fala5_do_1'] = rnd(s * (pr[5] - pr[4]) / w1, 3)
    if a1 is not None and len(a1) > 20 and uniewaznienie is not None:
        a_ = atr(a1[:, H], a1[:, L], a1[:, C])
        if a_ and np.isfinite(a_) and a_ > 0:
            out['elliott_do_uniewaznienia_atr'] = rnd(s * (float(a1[-1, C]) - uniewaznienie) / a_, 2)
    return out


def elliott_vs_setup(side, f):
    """V6.75: relacja kierunku setupu do rozpoznanej struktury 1h (tylko informacja)."""
    faza, kier = f.get('elliott_faza'), f.get('elliott_kierunek')
    if faza not in ('IMPULS_ROZWIJA_SIE', 'IMPULS_5_POTWIERDZONY') or kier not in ('LONG', 'SHORT'):
        return 'BRAK'
    zgodny = side == kier
    if faza == 'IMPULS_ROZWIJA_SIE':
        return 'ZGODNY_Z_ROZWIJAJACYM' if zgodny else 'PRZECIW_ROZWIJAJACEMU'
    return 'ZGODNY_Z_ZAKONCZONYM' if zgodny else 'PO_IMPULSIE'


def test_elliott_structure():
    # Przy ustalonej chwili oceny kolejne swiece nie moga zmieniac wczesniejszej klasyfikacji.
    n = 95
    a = np.zeros((n, 8), dtype=float)
    a[:, T] = np.arange(n) * HOUR_MS
    x = 100 + np.sin(np.arange(n) * 0.42) * 4 + np.arange(n) * 0.04
    a[:, O], a[:, H], a[:, L], a[:, C] = x, x + 1, x - 1, x
    short = {'1h': a[:75].copy(), '4h': a[::4][:18].copy()}
    before = elliott_struktura(short)
    later = a.copy(); later[75:, H] += 200; later[75:, L] -= 50
    assert before == elliott_struktura({'1h': later[:75], '4h': later[::4][:18]})
    assert all(t <= short['1h'][-1, T] - ELLIOTT_PRAWY * HOUR_MS for t, *_ in elliott_pivoty(short['1h']))
    assert elliott_struktura({'1h': a[:9], '4h': a[:4]})['elliott_faza'] == 'NIEJEDNOZNACZNE'
    # V6.73: przypadki pozytywne i negatywne na syntetycznych falach (LONG, SHORT, fala 3 najkrotsza, fala 4 w fali 1)
    def fale(punkty, krok=8):
        xs = []
        for p0, p1 in zip(punkty[:-1], punkty[1:]):
            xs += list(np.linspace(p0, p1, krok, endpoint=False))
        xs += [punkty[-1]] * 6
        m = len(xs)
        b = np.zeros((m + 30, 8), dtype=float)
        xx = np.r_[np.linspace(punkty[0] + 0.5 * (punkty[1] - punkty[0]), punkty[0], 30, endpoint=False), xs]   # dojscie do startu fali 1
        b[:, T] = np.arange(len(xx)) * HOUR_MS
        b[:, O], b[:, H], b[:, L], b[:, C] = xx, xx + 0.05, xx - 0.05, xx
        return {'1h': b, '4h': b[::4]}
    imp_up = fale([100, 110, 104, 125, 116, 131, 120])           # 1-2-3-4-5 + poczatek korekty (potwierdza szczyt 5)
    r_up = elliott_struktura(imp_up)
    assert r_up['elliott_faza'] == 'IMPULS_5_POTWIERDZONY' and r_up['elliott_kierunek'] == 'LONG', r_up
    assert abs(r_up['elliott_uniewaznienie'] - (116 - 0.05)) < 1e-6, r_up
    imp_dn = fale([200, 190, 196, 175, 184, 169, 180])
    r_dn = elliott_struktura(imp_dn)
    assert r_dn['elliott_faza'] == 'IMPULS_5_POTWIERDZONY' and r_dn['elliott_kierunek'] == 'SHORT', r_dn
    krotka3 = fale([100, 115, 108, 117, 112, 130, 120])         # fala 3 (9) najkrotsza
    assert elliott_struktura(krotka3)['elliott_faza'] == 'NIEJEDNOZNACZNE'
    nakladka = fale([100, 110, 104, 125, 108, 131, 120])        # fala 4 (108) w obszarze fali 1 (110)
    assert elliott_struktura(nakladka)['elliott_faza'] == 'NIEJEDNOZNACZNE'
    # V6.74: nowa zmiennosc po historycznym punkcie odciecia nie moze
    # przeliczyc starych pivotow (ich ATR byl juz znany przy potwierdzeniu).
    base = a[:75].copy()
    ext = a.copy()
    ext[75:, H] += 170
    ext[75:, L] -= 70
    early_pivots = elliott_pivoty(base)
    extended_pivots = elliott_pivoty(ext)
    cutoff = int(base[-1, T]) - ELLIOTT_PRAWY * HOUR_MS
    # Po wydluzeniu danych moga pojawic sie kolejne pivoty, a poprzedni
    # pivot tego samego typu moze zostac zastapiony ekstremalniejszym.
    # Dlatego porownujemy decyzje na dokladnie tym samym prefiksie danych.
    assert elliott_pivoty(ext[:75]) == early_pivots
    assert all(t <= cutoff for t, *_ in early_pivots)
    # Lokalny prog ruchu nie moze zalezec od ostatniego ATR dla calej serii.
    assert 'atr_now' not in elliott_pivoty.__code__.co_varnames
    assert elliott_trend_pivotow([(1,'L',100),(2,'H',110),(3,'L',105),(4,'H',115)]) == 'LONG'
    assert elliott_trend_pivotow([(1,'H',110),(2,'L',100),(3,'H',105),(4,'L',95)]) == 'SHORT'
    assert elliott_trend_pivotow([]) == 'NIEJEDNOZNACZNE'
    assert 'elliott_zgodnosc_1h_4h' in r_up
    # V6.75: proporcje fal na impulsie wzrostowym (pivoty: 99.95 / 110.05 / 103.95 / 125.05 / 115.95 / 131.05)
    assert abs(r_up['elliott_fala2_proc'] - 60.4) < 0.1 and abs(r_up['elliott_fala3_do_1'] - 2.089) < 0.002, r_up
    assert abs(r_up['elliott_fala4_proc'] - 43.1) < 0.1 and abs(r_up['elliott_fala5_do_1'] - 1.495) < 0.002, r_up
    assert r_up['elliott_do_uniewaznienia_atr'] is not None and r_up['elliott_do_uniewaznienia_atr'] > 0, r_up
    assert r_dn['elliott_fala3_do_1'] is not None and r_dn['elliott_fala3_do_1'] > 0, r_dn
    assert elliott_vs_setup('LONG', r_up) == 'ZGODNY_Z_ZAKONCZONYM' and elliott_vs_setup('SHORT', r_up) == 'PO_IMPULSIE'
    assert elliott_vs_setup('LONG', {'elliott_faza': 'IMPULS_ROZWIJA_SIE', 'elliott_kierunek': 'SHORT'}) == 'PRZECIW_ROZWIJAJACEMU'
    assert elliott_vs_setup('LONG', {'elliott_faza': 'NIEJEDNOZNACZNE', 'elliott_kierunek': 'BRAK'}) == 'BRAK'
    return 'OK (15 grup przypadkow)'


def analyze(A, price, now_ms, fees, btc_A=None, light=False, crowd=None):
    """Pelna analiza jednego kontraktu: (cechy, lista setupow). Te sama funkcja na zywo i w tescie."""
    f, st1 = market_features(A, price, now_ms, btc_A, light)
    if not f['atr1'] or not f['atr5'] or not f['atr15']:
        return f, []
    targets = liquidity_targets(A['1h'], A['15m'], now_ms)
    f['liq'] = liq_clusters(A['1h'], price)
    f['funding_z_hist'] = (crowd or {}).get('funding_z')
    f.update(elliott_struktura(A))   # V6.73: TYLKO INFORMACJA, bez zmiany setupow
    setups = []
    for s in (1, -1):
        for typ, fn in (('REV_MSS', lambda: setup_rev_mss(s, A, f, price)),
                        ('CONT_SWEEP', lambda: setup_cont_sweep(s, A, f, price)),
                        ('PULLBACK', lambda: setup_pullback(s, A, f, st1, price)),
                        ('SWING4H', lambda: setup_swing4h(s, A, f, price)),
                        ('CROWD', lambda: setup_crowd(s, A, f, price)),
                        ('ODBICIE', lambda: setup_odbicie(s, A, f, price))):
            if typ not in SETUP_TYPES:
                continue
            raw = fn()
            if raw is None:
                continue
            if typ in TYLKO_MARKET_TYPY:               # V6.53
                rows = [finalize(typ, s, raw, f, price, fees, targets, 'MARKET')]
            else:
                rows = [finalize(typ, s, raw, f, price, fees, targets, 'LIMIT')]
            if typ not in TYLKO_MARKET_TYPY and MARKET_ENTRIES and in_market_zone(s, raw, price, f['atr1']):
                rows.append(finalize(typ, s, raw, f, price, fees, targets, 'MARKET'))
            for row in rows:
                if row is None:
                    continue
                for k in FEATURE_COLS:
                    v = f.get(k)
                    row[k] = rnd(v, 3) if isinstance(v, (float, np.floating)) else v
                for k in ELLIOTT_COLS:
                    row[k] = f.get(k)
                row['elliott_vs_setup'] = elliott_vs_setup(row.get('side'), f)   # V6.75
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


KS_KOLUMNY = ['ks_imb_1pct', 'ks_imb_2pct', 'ks_opor_x', 'ks_opor_cena', 'ks_opor_usdt', 'ks_wsparcie_x',
              'ks_wsparcie_cena', 'ks_wsparcie_usdt', 'ks_zasieg_tp', 'ks_mediana_usdt', 'ks_opor_obrot', 'ks_wsparcie_obrot']


def ksiazka_setup(depth, r, obrot_24h=None):
    """V6.61: cechy arkusza zlecen dla setupu (stan z chwili uruchomienia, tylko informacja)."""
    out = {k: None for k in KS_KOLUMNY}
    if not isinstance(depth, dict) or not depth.get('asks') or not depth.get('bids'):
        return out
    try:
        asks = np.array([[number(p), number(q)] for p, q, *_ in depth['asks']], dtype=float)
        bids = np.array([[number(p), number(q)] for p, q, *_ in depth['bids']], dtype=float)
        asks, bids = asks[np.argsort(asks[:, 0])], bids[np.argsort(-bids[:, 0])]
        mid = (asks[0, 0] + bids[0, 0]) / 2
        for pct in (1, 2):
            a = asks[asks[:, 0] <= mid * (1 + pct / 100)]; b = bids[bids[:, 0] >= mid * (1 - pct / 100)]
            av, bv = float((a[:, 0] * a[:, 1]).sum()), float((b[:, 0] * b[:, 1]).sum())
            out[f'ks_imb_{pct}pct'] = rnd((bv - av) / (bv + av), 3) if av + bv > 0 else None
        med = float(np.median(np.concatenate([asks[:, 0] * asks[:, 1], bids[:, 0] * bids[:, 1]])))
        out['ks_mediana_usdt'] = rnd(med, 1)
        e, sl, tp = float(r['entry']), float(r['sl']), float(r['tp1'])
        if r['side'] == 'LONG':
            droga = asks[(asks[:, 0] > max(e, mid)) & (asks[:, 0] < tp)]
            oslona = bids[(bids[:, 0] > sl) & (bids[:, 0] < min(e, mid))]
            out['ks_zasieg_tp'] = bool(asks[-1, 0] >= tp)
        else:
            droga = bids[(bids[:, 0] < min(e, mid)) & (bids[:, 0] > tp)]
            oslona = asks[(asks[:, 0] < sl) & (asks[:, 0] > max(e, mid))]
            out['ks_zasieg_tp'] = bool(bids[-1, 0] <= tp)
        for nm, x in (('opor', droga), ('wsparcie', oslona)):
            if len(x) and med > 0:
                v = x[:, 0] * x[:, 1]; i = int(np.argmax(v))
                out[f'ks_{nm}_x'], out[f'ks_{nm}_cena'], out[f'ks_{nm}_usdt'] = rnd(v[i] / med, 1), psig(x[i, 0], 7), rnd(v[i], 0)
                ob = _num(obrot_24h)
                if ob and ob > 0:
                    out[f'ks_{nm}_obrot'] = rnd(v[i] / ob, 4)
    except Exception:
        pass
    return out


def ksiazka_txt(r):
    """V6.61: krotki opis arkusza do raportu (tylko gdy jest sciana albo wyrazna przewaga strony)."""
    cz = []
    if (r.get('ks_opor_x') or 0) >= KSIAZKA_SCIANA_X:
        cz.append(f"sciana PRZED TP {r['ks_opor_cena']} ({r['ks_opor_x']}x, {r.get('ks_opor_usdt')} USDT)")
    if (r.get('ks_wsparcie_x') or 0) >= KSIAZKA_SCIANA_X:
        cz.append(f"sciana PRZED SL {r['ks_wsparcie_cena']} ({r['ks_wsparcie_x']}x)")
    imb = r.get('ks_imb_1pct')
    if imb is not None and abs(imb) >= 0.3:
        cz.append(f"1%: przewaga {'kupujacych' if imb > 0 else 'sprzedajacych'} {imb:+.2f}")
    if r.get('ks_zasieg_tp') is False:
        cz.append('arkusz nie siega TP')
    return ('ksiazka (info): ' + '; '.join(cz)) if cz else ''


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


def oi_raw_fields(d):
    """V6.16: OI jawnie - wartosc z API, przyjeta jednostka i mnoznik (test 19.10: ilosc bazowa blizej Gate niz kontrakty)."""
    oi = d.get('open_interest')
    oi = oi[0] if isinstance(oi, list) and oi else oi
    raw = number(pick(oi, 'openInterest', 'oi', 'amount')) if isinstance(oi, dict) else None
    return {'oi_api_surowe': raw, 'oi_jednostka_zalozona': 'ilosc bazowa', 'oi_mnoznik': 1,
            'oi_contractVal': number((d.get('meta') or {}).get('contractVal')) or None}


def oi_reliability(r):
    """V6.16: OI WEEX (USD) vs Gate.io: zgodne 0.2-5x, inaczej niezgodne; brak Gate = bez potwierdzenia."""
    w = r.get('oi_usdt') or r.get('oi_surowe_usdt')
    g = r.get('gate_oi_usd')
    if w and g:
        q = w / g
        r['oi_vs_gate'] = rnd(q, 2)
        r['oi_wiarygodnosc'] = 'zgodne z Gate' if 0.2 <= q <= 5 else 'niezgodne z Gate'
        if r['oi_wiarygodnosc'] == 'niezgodne z Gate' and r.get('oi_usdt'):   # V6.17: nie pokazuj niepewnego OI w USD
            r['oi_surowe_usdt'], r['oi_usdt'] = r['oi_usdt'], None
    else:
        r['oi_vs_gate'] = None
        r['oi_wiarygodnosc'] = 'bez potwierdzenia'


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


def historical_slip_for(P, ts=None, qv24=None):
    """V6.150: poslizg w tescie wstecznym wg wolumenu 24h kontraktu w chwili zdjecia (as-of, ze swiec),
    niezalezny od dzisiejszego booka. Brak wolumenu -> HISTORICAL_SLIP_BPS. Zawsze w [SLIP_BPS, SLIP_MAX_BPS]."""
    v = _num(qv24) if qv24 is not None else None
    out = float(HISTORICAL_SLIP_BPS)
    if v is not None and np.isfinite(v) and v > 0:
        for prog, bps in HISTORICAL_SLIP_PROGI:
            if v >= prog:
                out = float(bps)
                break
    return float(min(max(out, SLIP_BPS), SLIP_MAX_BPS))


def test_historical_slippage_v136():
    a={'raw': {'depth': {'bids': [['100','1000']], 'asks': [['101','1000']]}},
       '_slip': 3.0}
    b={'raw': {'depth': {'bids': [['80','1']], 'asks': [['120','1']]}},
       '_slip': 30.0}
    assert historical_slip_for(a, 1) == historical_slip_for(b, 9999999999)          # niezalezne od booka i czasu
    assert historical_slip_for(a) == float(HISTORICAL_SLIP_BPS)                     # brak wolumenu
    assert historical_slip_for(a, 1, 3e8) == 5.0 and historical_slip_for(b, 1, 3e8) == 5.0   # V6.150: progi
    assert historical_slip_for(a, 1, 2e7) == 10.0 and historical_slip_for(a, 1, 1e6) == 12.0
    assert all(SLIP_BPS <= historical_slip_for(a, 1, v) <= SLIP_MAX_BPS for v in (None, 0, 1e3, 1e7, 1e12))
    return 'OK (niezalezne od biezacego booka, progi wolumenu as-of, granice)'


def simulate(a5, ts, row, fees, fund=None):
    """V6.16: _simulate_core + kontrola luk w swiecach 5m od zdjecia do wyjscia/wygasniecia (luka -> 'luka_danych')."""
    res = _simulate_core(a5, ts, row, fees, fund)
    i0 = int(np.searchsorted(a5[:, T], ts))
    end = res.pop('_end_i', None)
    res['luka_danych'] = False
    if end is not None and end > i0:
        seg = a5[i0:end + 1, T]
        if len(seg) > 1 and (np.diff(seg) > INTERVAL_MS['5m'] * 1.5).any():
            res['luka_danych'] = True
            if res['status'] in FINAL or res['status'] in ('niewypelniony', 'anulowany'):
                res['status_przed_luka'] = res['status']
                res['status'] = 'luka_danych'
    return res


def _simulate_core(a5, ts, row, fees, fund=None):
    """LIMIT: wypelnienie, SL/TP1/timeout na swiecach 5m od ts. MARKET: wejscie po otwarciu pierwszej swiecy od ts.
    Pesymistycznie: SL przed TP w tej samej swiecy. V6.16: 'niejednoznaczne' = powod, gdy kolejnosci nie da sie ustalic."""
    s = 1 if row['side'] == 'LONG' else -1
    entry, sl, tp = row['entry'], row['sl'], row['tp1']
    taker, maker = fees
    i0 = int(np.searchsorted(a5[:, T], ts))
    wait_bars = int(row['wait_h'] * 12)
    hold_bars = SETUP_HOLD_H.get(row.get('setup_type'), BT_MAX_HOLD_H) * 12
    hold_bars_1r = max(hold_bars, (HOLD_PO_1R_H or 0) * 12)
    risk = abs(entry - sl)
    res = {'status': 'oczekuje', 'filled': False, 'r': 0.0, 'mfe_R': None, 'mae_R': None, 'fill_utc': None,
           'exit_utc': None, 'bars_held': None, 'niejednoznaczne': '', '_end_i': len(a5) - 1}
    amb = ''
    fi = exit_i = st = None
    market = row.get('entry_type') == 'MARKET'
    if market:
        if i0 >= len(a5):
            return res
        fi = i0
        entry = float(a5[i0, O])        # realna cena wejscia (poslizg wzgledem ceny ze zdjecia)
        if s * (entry - sl) <= 0 or s * (tp - entry) <= 0:
            # V6.58: cena juz za SL albo za TP1 - zlecenia by nie zlozono (wczesniej sztuczne -1R / sztuczny zysk)
            res.update(status='anulowany', exit_utc=snap_iso(a5[i0, T]), _end_i=i0, niejednoznaczne='luka MARKET')
            return res
        risk = abs(entry - sl)
    z1r = zarz_1r(row.get('setup_type'))  # V6.53: ODBICIE bez czesci na 1R
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
                res.update(status='niewypelniony', exit_utc=snap_iso(a5[i, T]), _end_i=i)
                return res
            px = entry * (1 - s * LIMIT_PRZEBICIE_BPS / 1e4)   # V6.11: cena musi PRZEJSC przez wejscie
            if not (lo <= px if s == 1 else hi >= px):
                if hit_tp:
                    res.update(status='anulowany', exit_utc=snap_iso(a5[i, T]), _end_i=i)
                    return res
                continue
            fi = i                       # wypelnienie; TP w tej samej swiecy nie liczy sie (nie wiemy, co pierwsze)
            if hit_sl:
                amb = 'wejscie+SL'       # V6.16: kolejnosc nieznana - liczone SL (pesymistycznie)
                st, exit_i = 'SL', i
                break
            if hit_tp:
                amb = 'wejscie+TP'       # V6.16: TP w swiecy wypelnienia pominiety (pesymistycznie)
            continue
        if hit_sl:
            if part_i is None:
                st = 'SL'
                if hit_tp:
                    amb = 'SL+TP'            # V6.16: obie w jednej swiecy - liczone SL
                elif z1r and (hi >= entry + risk if s == 1 else lo <= entry - risk):
                    amb = 'SL+1R'            # V6.16: 1R i SL w jednej swiecy - liczone SL bez czesci
            else:
                st = 'TRAIL' if s * (stop - entry) > 0 else 'BE'
                trail_px = stop
            exit_i = i
            break
        if z1r and part_i is None:
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
    res['niejednoznaczne'] = amb
    if fi is None:
        return res                       # dane skonczyly sie przed wypelnieniem
    end = exit_i if exit_i is not None else len(a5) - 1
    res['_end_i'] = end
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


def sim_fixed(a5, ts, s, sl_d, tp_d, fees, fee_in=None, hold_h=None, z1r=None):
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
    if (ZARZADZANIE_1R if z1r is None else z1r):
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
                          hold_h=SETUP_HOLD_H.get(r.get('setup_type'), BT_MAX_HOLD_H), z1r=zarz_1r(r.get('setup_type')))
            if x is not None:
                res.append(x)
        if res:
            r['control_R'] = rnd(float(np.mean(res)), 3)
            r['edge_R'] = rnd(r['r'] - r['control_R'], 3)


_bt_stat = {'trafienia': 0, 'liczone': 0}


def _bt_odcisk(A):
    """V6.98: suma kontrolna okien swiec (wszystkie interwaly) - inne swiece = inny klucz."""
    return tuple((tf, len(A[tf]), zlib.crc32(np.ascontiguousarray(A[tf]).tobytes())) for tf in sorted(A))


def _kod_odcisk():
    """V6.101: odcisk TYLKO tego, od czego zalezy wynik analyze() w tescie wstecznym: funkcje osiagalne z analyze
    (rekurencyjnie po nazwach globalnych, takze funkcje schowane w slownikach/listach) i stale, ktorych uzywaja.
    Zmiana raportow, VERSION albo innych funkcji nie kasuje pamieci; zmiana logiki setupow albo ustawien - kasuje.
    Zbiory sa sortowane, a obiekty bez stalej reprezentacji zastapione nazwa typu, wiec odcisk jest TAKI SAM
    w kazdej sesji Colab (wczesniej frozenset w stalych dawal inny odcisk w nowej sesji -> plik z Drive odrzucany)."""
    import types as _t
    h = hashlib.sha1(b'bt-analiza-v6.101')
    plik = run_backtest.__code__.co_filename
    g = globals()
    kolejka, odw = ['analyze'], set()

    def wart(v):
        if isinstance(v, _t.FunctionType):
            if v.__code__.co_filename == plik:
                kolejka.append(v)
            return 'fn:' + v.__qualname__
        if isinstance(v, (set, frozenset)):
            return '{' + ','.join(sorted(wart(x) for x in v)) + '}'
        if isinstance(v, dict):
            return '{' + ','.join(wart(k) + ':' + wart(x) for k, x in v.items()) + '}'
        if isinstance(v, (list, tuple)):
            return '[' + ','.join(wart(x) for x in v) + ']'
        if v is None or isinstance(v, (bool, int, float, str, bytes, complex)):
            return repr(v)
        return '<' + type(v).__name__ + '>'

    def kod(c):
        h.update(c.co_code)
        for k in c.co_consts:
            if isinstance(k, _t.CodeType):
                kod(k)
            else:
                h.update(wart(k).encode())
        h.update(repr(c.co_names).encode())
        kolejka.extend(c.co_names)

    while kolejka:
        el = kolejka.pop()
        fn = el if isinstance(el, _t.FunctionType) else None
        nazwa = el.__name__ if fn else el
        if fn is None:
            v = g.get(nazwa, None)
            if isinstance(v, _t.FunctionType):
                fn = v
        klucz = id(fn) if fn else nazwa
        if klucz in odw:
            continue
        odw.add(klucz)
        if fn is not None:
            if fn.__code__.co_filename != plik:
                continue
            h.update(('F:' + fn.__qualname__).encode())
            kod(fn.__code__)
            h.update(wart(fn.__defaults__ or ()).encode())
            continue
        if nazwa not in g or nazwa.startswith('_') or nazwa in ('REPLAY_ZIP', 'DRIVE_DIR', 'VERSION'):
            continue
        v = g[nazwa]
        if nazwa.isupper() or (v is None or isinstance(v, (bool, int, float, str, tuple, frozenset))):
            h.update(('K:' + nazwa + '=').encode()); h.update(wart(v).encode())
    return h.hexdigest()


def _bt_magazyn():
    import builtins
    if not hasattr(builtins, '_weex_bt_cache'):          # przezywa kolejne exec() w tej samej sesji Colab
        builtins._weex_bt_cache = {}
    return builtins._weex_bt_cache


def _bt_cache_slownik(snaps):
    """Slownik pamieci dla biezacego odcisku kodu; stare zdjecia (poza oknem testu) usuwane."""
    m = _bt_magazyn()
    kh = _kod_odcisk()
    for k in [k for k in m if k != kh]:
        del m[k]
    d = m.setdefault(kh, {})
    okno = set(snaps)
    for k in [k for k in d if k[1] not in okno]:
        del d[k]
    return d


def _bt_key_to_json(k):
    return json.loads(json.dumps(k))  # rowniez zagniezdzone tuple


def _bt_key_from_json(obj):
    # Przywraca hashable klucze, w tym zagniezdzone odciski swiec.
    if isinstance(obj, list):
        return tuple(_bt_key_from_json(x) for x in obj)
    if isinstance(obj, (str, int, float, bool)) or obj is None:
        return obj
    raise ValueError('Niedozwolony typ klucza cache')


def _bt_cache_encode(value):
    return json.dumps(value, default=json_default, ensure_ascii=False,
                      allow_nan=False, separators=(',', ':')).encode('utf-8')


def _bt_cache_decode(value):
    return json.loads(value)


def bt_cache_wczytaj(wolno):
    """V6.99: tylko JSON (bez wykonywania kodu z plikow Drive)."""
    if not (BT_CACHE and wolno):
        _bt_cache_io['status'] = 'wylaczony_lub_bez_drive'
        return
    _read_start = time.monotonic()
    m = _bt_magazyn()
    kh = _kod_odcisk()
    if m.get(kh):
        _bt_cache_io.update(status='pamiec_sesji', wpisy=len(m[kh]), odczyt_s=round(time.monotonic() - _read_start, 3))
        print(f"  pamiec testu wstecznego: {len(m[kh])} analiz w tej sesji")
        return
    fp = Path(DRIVE_DIR) / BT_CACHE_FILE
    try:
        if fp.exists():
            compressed = fp.read_bytes()
            _bt_cache_io['rozmiar_b'] = len(compressed)
            if len(compressed) > 80_000_000:
                raise ValueError('Cache przekracza limit 80 MB')
            decomp = zlib.decompressobj()
            raw = decomp.decompress(compressed, 320_000_001)
            if len(raw) > 320_000_000 or decomp.unconsumed_tail or not decomp.eof:
                raise ValueError('Cache przekracza limit 320 MB lub jest niekompletny')
            dane = json.loads(raw)
            if isinstance(dane, dict) and dane.get('kod') == kh and isinstance(dane.get('d'), list):
                entries = {}
                for item in dane['d']:
                    if not isinstance(item, list) or len(item) != 2:
                        raise ValueError('Nieprawidlowy rekord cache')
                    key, value = item
                    if not isinstance(value, list) or len(value) != 2:
                        raise ValueError('Nieprawidlowa wartosc cache')
                    entries[_bt_key_from_json(key)] = _bt_cache_encode(value)
                m.clear(); m[kh] = entries
                _bt_cache_io.update(status='wczytany_drive', wpisy=len(entries))
                print(f"  pamiec testu wstecznego z Drive: {len(entries)} analiz")
            else:
                _bt_cache_io['status'] = 'inny_odcisk_kodu'
                _bt_cache_io['odcisk_zapisany'] = str(dane.get('kod', ''))[:40] if isinstance(dane, dict) else ''
                _bt_cache_io['odcisk_biezacy'] = kh
                print('  pamiec testu wstecznego z Drive: zmieniona logika analizy lub konfiguracja - liczenie od nowa')
        else:
            _bt_cache_io['status'] = 'brak_pliku'
    except Exception as exc:
        _bt_cache_io['status'] = 'blad_odczytu: ' + str(exc)[:120]
        print(f'  pamiec testu wstecznego: blad odczytu ({str(exc)[:100]}) - liczenie od nowa')
    finally:
        _bt_cache_io['odczyt_s'] = round(time.monotonic() - _read_start, 3)


def bt_cache_zapisz(wolno):
    if not (BT_CACHE and wolno):
        return
    m = _bt_magazyn()
    kh = _kod_odcisk()
    if not m.get(kh):
        return
    _save_start = time.monotonic()
    try:
        rows = [[_bt_key_to_json(k), _bt_cache_decode(v)] for k, v in m[kh].items()]
        blob = zlib.compress(_bt_cache_encode({'kod': kh, 'd': rows}), 1)
        zapisz_atomowo(Path(DRIVE_DIR) / BT_CACHE_FILE, lambda tmp: Path(tmp).write_bytes(blob), kopia=False)
        _bt_cache_io.update(zapis_s=round(time.monotonic() - _save_start, 3), rozmiar_b=len(blob), wpisy=len(rows))
    except Exception as exc:
        _bt_cache_io['blad_zapisu'] = str(exc)[:120]
        print(f'  pamiec testu wstecznego: blad zapisu ({str(exc)[:100]})')


def run_backtest(PP, now_ms, lookback_h=None, quiet=False, oi_arr=None, gidx=None, pamiec=False):
    lookback_h = lookback_h or BT_LOOKBACK_H
    oi_arr, gidx = oi_arr or {}, gidx or {}
    btc = PP.get('BTCUSDT')
    step = BT_STEP_H * HOUR_MS
    last_snap = (now_ms // step) * step
    snaps = [last_snap - k * step for k in range(int(lookback_h / BT_STEP_H), -1, -1)]
    out, seen = [], set()
    t0 = time.time()
    cache = _bt_cache_slownik(snaps) if (BT_CACHE and pamiec) else None   # V6.98: tylko glowny test 7 dni
    if pamiec:                                       # V6.102: liczniki tylko z glownego testu 7 dni
        _bt_stat.update(trafienia=0, liczone=0)
    for n, ts in enumerate(snaps):
        btc_A = view(btc, ts) if btc else None
        btc_fp = _bt_odcisk(btc_A) if (cache is not None and btc_A) else None
        snap_rows = []
        chg, atr_at, qv24 = {}, {}, {}
        for sym, P in PP.items():
            if '5m' not in P['arr'] or (P['asset_type'] in STOCK_TYPES and not (TRADFI_ON and (TRADFI_24H or us_session_open(ts)))):
                continue                                   # V6.14: akcje/indeksy tylko w sesji USA; V6.88: cala doba
            A = view(P, ts)
            if not enough(A):
                continue
            if P['asset_type'] in STOCK_TYPES and TRADFI_24H and not us_session_open(ts) and swiece_martwe(A['5m']):
                continue                                   # V6.88: rynek faktycznie zamkniety (brak wolumenu/zakresu)
            price = float(A['5m'][-1, C])
            fz_ = funding_z_at(P.get('raw'), ts)
            klucz_ = (sym, ts, _bt_odcisk(A), btc_fp if sym != 'BTCUSDT' else None, fz_, repr(P['fees'])) if cache is not None else None
            traf_ = cache.get(klucz_) if cache is not None else None
            if traf_ is not None:                          # V6.98: ta sama analiza co w poprzednim uruchomieniu
                f, setups = _bt_cache_decode(traf_)
                if pamiec: _bt_stat['trafienia'] += 1
            else:
                f, setups = analyze(A, price, ts, P['fees'], btc_A if sym != 'BTCUSDT' else None, light=True,
                                    crowd={'funding_z': fz_})
                if cache is not None:                      # zapis PRZED modyfikacja setupow ponizej
                    f = {'chg_24h_pct': f.get('chg_24h_pct'), 'atr1': f.get('atr1')}
                    cache[klucz_] = _bt_cache_encode([f, setups])
                    setups = _bt_cache_decode(cache[klucz_])[1]
                if pamiec: _bt_stat['liczone'] += 1
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
                         slip_bps=rnd(historical_slip_for(P, ts, qv24.get(sym)), 2), slip_source=HISTORICAL_SLIP_MODEL,
                         metoda_sym='v150_tier_qv24')
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
    keys = keys + ('SETUP_HOLD_H', 'ODBICIE_PO_POMPIE_PCT', 'ODBICIE_KLUCZ_H', 'ODBICIE_SPADEK_PCT', 'ODBICIE_SL_ATR', 'ODBICIE_TP_ATR', 'ODBICIE_MAX_RISK_PCT', 'BEZ_1R_TYPY')   # V6.53
    return json.dumps({**{k: globals()[k] for k in keys}, 'kontrola': 'v2-chwila-wejscia', 'liq': 'v1', 'fix5': 'v1', 'konfluencje': 'v1', 'v616': 'niejednoznaczne+luki', 'zarzadzanie_1R': ZARZADZANIE_1R, 'czesc_1R': CZESC_1R, 'trail_1R': TRAIL_PO_1R_R, 'hold_1R': HOLD_PO_1R_H, 'azja_short': FILTR_AZJA_SHORT, 'short_godziny': list(FILTR_SHORT_GODZINY_UTC), 'fvg4h': SWING4H_BEZ_FVG4H, 'wylaczone': sorted(map(list, WYLACZONE_TYPY)), 'wylaczone_wejscia': sorted(map(list, WYLACZONE_WEJSCIA)), 'funding': ['v2', FUNDING_LIMIT], 'v611': [LIMIT_PRZEBICIE_BPS, FUNDING_W_WYNIKU, SLIP_DYNAMIC, SLIP_MAX_BPS, ORDER_SIZE_USDT]}, sort_keys=True, default=str)   # 'konfluencje': cache testu z 14.10 nadal wazny


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


def hist_cache_wazny(computed_ms, now_ms):
    """V6.51: cache testu 30 dni wazny, chyba ze (a) starszy niz HIST_REFRESH_MAX_H albo (b) teraz jest okno
    HIST_OKNO_UTC, a cache policzono przed poczatkiem dzisiejszego okna (i co najmniej 12 h temu)."""
    if not computed_ms:
        return False
    wiek_h = (now_ms - computed_ms) / HOUR_MS
    if wiek_h >= HIST_REFRESH_MAX_H:
        return False
    od, do = HIST_OKNO_UTC
    godz = (now_ms // HOUR_MS) % 24
    if od <= godz < do:
        start_okna = (now_ms // DAY_MS) * DAY_MS + od * HOUR_MS
        if computed_ms < start_okna and wiek_h >= min(12, HIST_REFRESH_H):
            return False
    return True


def _warianty_podzbior(stary_sig, nowy_sig):
    """V6.51: czy nowa lista wariantow jest podzbiorem policzonej (te same ustawienia i metoda)."""
    try:
        a, b = json.loads(stary_sig or '{}'), json.loads(nowy_sig)
    except Exception:
        return False
    if a.get('metoda') != b.get('metoda'):
        return False
    wa, wb = a.get('warianty') or {}, b.get('warianty') or {}
    return bool(wb) and all(k in wa and wa[k] == v for k, v in wb.items())


def test_hist_okno():
    """V6.51: waznosc cache testu 30 dni i podzbior wariantow."""
    t = lambda s_: parse_utc(s_)
    przyp = [
        ('poranek w oknie, cache z wczoraj rano', '2026-10-22T04:30', '2026-10-23T04:00', False),
        ('w oknie, cache z dzis z okna', '2026-10-23T02:30', '2026-10-23T05:00', True),
        ('poza oknem, cache 20 h', '2026-10-22T16:00', '2026-10-23T12:00', True),
        ('poza oknem, cache 40 h', '2026-10-21T20:00', '2026-10-23T12:00', False),
        ('w oknie, cache 3 h (z wczoraj w nocy)', '2026-10-23T00:30', '2026-10-23T03:30', True),
    ]
    for nazwa, c, n, oczek in przyp:
        assert hist_cache_wazny(t(c), t(n)) == oczek, f'test okna: {nazwa}'
    s1 = json.dumps({'warianty': {'a': {'X': 1}, 'b': {'Y': 2}}, 'metoda': 'm'}, sort_keys=True)
    assert _warianty_podzbior(s1, json.dumps({'warianty': {'a': {'X': 1}}, 'metoda': 'm'}, sort_keys=True)), 'test podzbioru 1'
    assert not _warianty_podzbior(s1, json.dumps({'warianty': {'a': {'X': 2}}, 'metoda': 'm'}, sort_keys=True)), 'test podzbioru 2'
    assert not _warianty_podzbior(s1, json.dumps({'warianty': {'c': {'Z': 1}}, 'metoda': 'm'}, sort_keys=True)), 'test podzbioru 3'
    return 'OK (8 przypadkow)'


def run_hist(PP, now_ms, drive_ok):
    """Test definicji setupow na HIST_DAYS dniach swiec (prowizje WEEX) + warianty ustawien (HIST_WARIANTY).
    Wynik w cache na HIST_REFRESH_H. Zwraca (df, meta); tabela wariantow w meta['_warianty']."""
    cache = Path(DRIVE_DIR) if drive_ok else OUT_ROOT / 'weex_cache'
    cache.mkdir(parents=True, exist_ok=True)
    meta_p, data_p = cache / 'hist_meta_v63.json', cache / HIST_NAME
    var_p = cache / 'hist_warianty_v64.csv'
    sig = hist_signature()
    var_sig = json.dumps({'warianty': HIST_WARIANTY, 'metoda': 'v627_to_samo_okno'}, sort_keys=True, default=str)
    df = m = None
    if meta_p.exists() and data_p.exists():
        try:
            mm = json.loads(meta_p.read_text())
            if mm.get('sig') == sig and hist_cache_wazny(mm.get('computed_ms', 0), now_ms):   # V6.51
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
    elif m is not None and var_p.exists() and _warianty_podzbior(m.get('var_sig'), var_sig):   # V6.51
        try:
            var_df = pd.read_csv(var_p)
            var_df = var_df[var_df.wariant.isin(['BAZA'] + list(HIST_WARIANTY))]
            var_df.to_csv(var_p, index=False)
            m['var_sig'] = var_sig
            meta_p.write_text(json.dumps({k: v for k, v in m.items() if not k.startswith('_')}))
            print('  warianty: mniej niz w cache - wyniki z cache, bez przeliczania')
        except Exception:
            var_df = None
    if df is not None and (var_df is not None or not HIST_WARIANTY):
        m['_warianty'] = var_df
        return df, m
    if df is not None and HIST_WARIANTY:                # V6.27: warianty tylko razem z baza (to samo okno i swiece)
        print('  warianty zmienione - przeliczam baze i warianty na tym samym oknie')
        df = m = None
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
        var_df['okno_utc'] = iso(now_ms)[:16]             # V6.27
        var_df.to_csv(var_p, index=False)
        m['var_sig'] = var_sig
    meta_p.write_text(json.dumps({k: v for k, v in m.items() if not k.startswith('_')}))
    m['_warianty'] = var_df
    return df, m


def hist_table(df, od=None):
    """Wyniki testu historycznego per typ+wejscie i kierunek (tylko setupy po filtrach)."""
    rows = []
    if not len(df):
        return pd.DataFrame()
    ok = df[df.filtr.fillna('') == '']
    if 'okazja_id' not in ok.columns:
        ok = add_event_ids(ok.copy())
    for (tv, side), d in ok.groupby(['typ_wersja', 'side']):
        st = stats(d)
        if not st.get('transakcje'):
            continue
        lq = liq_compare(d)
        rows.append({'typ': tv, 'side': side, 'transakcje': st.get('transakcje', 0), 'sredni_R': st.get('sredni_R'),
                     'losowe_R': st.get('losowe_R'), 'sredni_edge_R': st.get('sredni_edge_R'), 't_edge': st.get('t_edge'),
                     'skutecznosc_pct': st.get('skutecznosc_pct'), 'zdjecia': st.get('zdjecia'),
                     'unikalne_okazje': int(d[d.status.isin(FINAL)].okazja_id.nunique()),
                     **_po_reg_cols(d, od_strona(od, tv, side) if od else None), **lq})
    return pd.DataFrame(rows)


def _po_reg_cols(d, since):
    """V6.21: wynik czesci testu po ustaleniu regul (zdjecia >= since)."""
    if not since:
        return {'po_reg_od': None, 'po_reg_n': 0, 'po_reg_R': None, 'po_reg_edge': None}
    x = d[(d.snapshot_utc.astype(str).str[:16] >= since) & d.status.isin(FINAL)]
    e = pd.to_numeric(x['edge_R'], errors='coerce') if 'edge_R' in x.columns else pd.Series(dtype=float)
    return {'po_reg_od': since, 'po_reg_n': int(len(x)), 'po_reg_R': rnd(pd.to_numeric(x.r, errors='coerce').mean(), 3) if len(x) else None,
            'po_reg_edge': rnd(e.mean(), 3) if e.notna().any() else None}


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
            'sredni_R_zdj': rnd(per.mean(), 3), 'sredni_edge_R_zdj': rnd(pe.mean(), 3) if len(pe) else None,   # V6.72
            'wypelnienie_pct': rnd(df.filled.astype(str).str.lower().isin(['true', '1', '1.0']).mean() * 100, 1)}


def progi_R(st):
    """V6.72: (sredni R, srednia przewaga) do progow bramki - z waga zdjecia rynku (GATE_WAGA_ZDJECIA) albo po transakcjach."""
    if GATE_WAGA_ZDJECIA:
        return st.get('sredni_R_zdj'), st.get('sredni_edge_R_zdj')
    return st.get('sredni_R'), st.get('sredni_edge_R')


def add_event_ids(df):
    """V6.16: stabilne ID. okazja_id = symbol|typ|kierunek|strefa (bez MARKET/LIMIT i wersji); event_id = + wersja + wejscie."""
    if not len(df) or 'symbol' not in df.columns:
        return df
    zone = pd.to_numeric(df['zone_ref'] if 'zone_ref' in df.columns else df['entry'], errors='coerce')
    zone = zone.fillna(pd.to_numeric(df['entry'], errors='coerce')).map(lambda v: f'{psig(float(v), 7):.10g}' if pd.notna(v) else '')
    typ = df['setup_type'] if 'setup_type' in df.columns else df['typ_wersja'].astype(str).str.split('.').str[0]
    key = df['symbol'].astype(str) + '|' + typ.astype(str) + '|' + df['side'].astype(str) + '|' + zone
    df['okazja_id'] = key.map(lambda k: hashlib.sha1(k.encode()).hexdigest()[:12])
    ev = df['okazja_id'] + '|' + df['typ_wersja'].astype(str) + '|' + df['entry_type'].astype(str)
    df['event_id'] = ev.map(lambda k: hashlib.sha1(k.encode()).hexdigest()[:12])
    return df


def _flag(df, col):
    if col not in df.columns:
        return pd.Series(False, index=df.index)
    v = df[col]
    if v.dtype == bool:
        return v
    return v.fillna('').astype(str).str.strip().str.lower().isin(['true', '1', '1.0']) if col == 'luka_danych' else \
        v.fillna('').astype(str).str.strip().ne('')


REGULY_OD_PLIK = 'reguly_od.json'
REGULY_OD_STRONA = {('CONT_SWEEP.v4', 'LONG'): '2026-10-26T08:00',    # V6.58: zmiana wejscia LONG w V6.57;
                    ('CONT_SWEEP.v4', 'SHORT'): '2026-10-26T08:00',   # V6.71: CONT_SL_ATR 0.8 -> 1.2 = nowe reguly
                    ('CONT_SWEEP.v4.M', 'LONG'): '2026-10-26T08:00',  # (statystyka bramki/proby CONT_SWEEP od nowa)
                    ('CONT_SWEEP.v4.M', 'SHORT'): '2026-10-26T08:00'}


def od_strona(od, tv, side, base=None):
    """V6.58: data startu statystyki dla typ_wersja x kierunek (pozniejsza z daty wersji i REGULY_OD_STRONA)."""
    v = (od or {}).get(tv, base)
    w = REGULY_OD_STRONA.get((tv, side))
    if w and (v is None or w > v):
        return w
    return v
REGULY_OD_ZASIEW = {'ODBICIE.v1': '2026-10-23T18:13', 'ODBICIE.v1.M': '2026-10-23T18:13'}   # V6.55


def reguly_od(arch, trwale=False):
    """V6.16: od kiedy dana wersja typu dziala na zywo, nie wczesniej niz GATE_FROM_UTC.
    V6.55: pierwsza data zapamietywana na Drive (minimum z zapisanej, zasiewu i archiwum) - archiwum nadpisuje
    run_utc ostatnich 7 dni, wiec samo archiwum przesuwalo date nowych typow na kazde uruchomienie."""
    pierwsze = dict(REGULY_OD_ZASIEW)
    fp = Path(DRIVE_DIR) / REGULY_OD_PLIK
    if trwale and fp.exists():
        try:
            for k, v in json.loads(fp.read_text()).items():
                pierwsze[k] = min(str(v)[:16], pierwsze.get(k, '9999'))
        except Exception as exc:
            print(f'{REGULY_OD_PLIK}: blad odczytu ({str(exc)[:80]})')
    if arch is not None and len(arch) and 'run_utc' in arch.columns:
        for tv, g in arch.groupby('typ_wersja'):
            pierwsze[tv] = min(str(g.run_utc.min())[:16], pierwsze.get(tv, '9999'))
    if trwale:
        try:
            fp.parent.mkdir(parents=True, exist_ok=True)
            zapisz_atomowo(fp, lambda t: t.write_text(json.dumps(pierwsze, sort_keys=True)))   # V6.59
        except Exception as exc:
            print(f'{REGULY_OD_PLIK}: blad zapisu ({str(exc)[:80]})')
    return {tv: max(GATE_FROM_UTC, v) for tv, v in pierwsze.items()}


def test_zapisu():
    """V6.59: zapis atomowy + .bak; uszkodzone archiwum -> odczyt z .bak; oba nieczytelne -> archiwum nie nadpisane."""
    import tempfile, contextlib
    global DRIVE_DIR
    stary = DRIVE_DIR
    w = []
    try:
        with tempfile.TemporaryDirectory() as td:
            DRIVE_DIR = td
            p = Path(td) / 'x.csv'
            zapisz_atomowo(p, lambda t: pd.DataFrame({'a': [1]}).to_csv(t, index=False))
            zapisz_atomowo(p, lambda t: pd.DataFrame({'a': [1, 2]}).to_csv(t, index=False))
            w.append(len(pd.read_csv(p)) == 2 and len(pd.read_csv(p.with_name('x.csv.bak'))) == 1
                     and not p.with_name('x.csv.tmp').exists())                                  # 1: zapis + kopia
            ap = Path(td) / ARCHIVE_NAME
            row = {'snapshot_utc': snap_iso(int(time.time() * 1000) - 3600_000), 'symbol': 'AUSDT', 'typ_wersja': 'PULLBACK.v4',
                   'side': 'LONG', 'entry': 1.0, 'zone_ref': 1.0, 'status': 'SL', 'r': -1.0, 'entry_type': 'LIMIT',
                   'setup_type': 'PULLBACK', 'sl': 0.9, 'tp1': 1.2, 'filtr': '', 'filled': True}
            now = int(time.time() * 1000)
            with contextlib.redirect_stdout(io.StringIO()):
                update_archive([row], '2026-10-24T10:00', True, now)
                update_archive([{**row, 'symbol': 'BUSDT'}], '2026-10-24T11:00', True, now)
                ap.write_bytes(b'\x00\xff"zepsute')                                          # uszkodzony plik glowny
                d = update_archive([{**row, 'symbol': 'CUSDT'}], '2026-10-24T12:00', True, now)
            w.append(not _ARCH_BLAD and set(d.symbol) >= {'AUSDT', 'CUSDT'})                       # 2: odczyt z .bak
            ap.write_bytes(b'\x00\xff"zepsute'); ap.with_name(ap.name + '.bak').write_bytes(b'\x00\xff"zepsute')
            with contextlib.redirect_stdout(io.StringIO()):
                update_archive([{**row, 'symbol': 'DUSDT'}], '2026-10-24T13:00', True, now)
            w.append(bool(_ARCH_BLAD) and ap.read_bytes().startswith(b'\x00'))                   # 3: nie nadpisane
            _ARCH_BLAD.clear()
    finally:
        DRIVE_DIR = stary
    assert all(w), f'test_zapisu: {w}'
    return f'OK ({len(w)} przypadki)'


def test_waga_zdjec():
    """V6.72: zdjecie z 30 wygranymi i 20 zdjec z 1 strata: po transakcjach sredni R > 0, z waga zdjecia < 0 -> OBSERWACJA."""
    rows = []
    for k in range(30):
        rows.append({'snapshot_utc': '2026-10-20T10:00', 'r': 1.0, 'edge_R': 0.5, 'control_R': 0.5, 'status': 'TP1', 'filled': True})
    for j in range(20):                                # 20 zdjec po 1 stracie: po transakcjach +0.2R, po zdjeciach -0.9R
        rows.append({'snapshot_utc': f'2026-10-2{1 + j // 10}T{10 + j % 10:02d}:00', 'r': -1.0, 'edge_R': -0.2,
                     'control_R': -0.8, 'status': 'SL', 'filled': True})
    st = stats(pd.DataFrame(rows))
    assert st['sredni_R'] > 0 and st['sredni_R_zdj'] < 0, f'test wagi zdjec: {st}'
    assert progi_R(st)[0] == (st['sredni_R_zdj'] if GATE_WAGA_ZDJECIA else st['sredni_R']), 'test wagi zdjec: progi_R'
    base = {'typ_wersja': 'PULLBACK.v4', 'side': 'LONG', 'filtr': '', 'symbol': 'XUSDT', 'setup_type': 'PULLBACK',
            'entry_type': 'LIMIT', 'zone_ref': 1.0, 'entry': 1.0}
    arch = pd.DataFrame([{**base, **x, 'zone_ref': 1.0 + i * 1e-4} for i, x in enumerate(rows)])
    g = gate_table(arch, parse_utc('2026-10-26T10:00'), {'PULLBACK.v4': '2026-10-15T08:00'})
    g = g[(g.typ == 'PULLBACK.v4') & (g.side == 'LONG')].iloc[0]
    assert g.transakcje == 50 and g.status == ('OBSERWACJA' if GATE_WAGA_ZDJECIA else g.status), f'test wagi zdjec: {g.to_dict()}'
    assert g.sredni_R_transakcje > 0 and (g.sredni_R < 0 if GATE_WAGA_ZDJECIA else True), f'test wagi zdjec: {g.to_dict()}'
    return 'OK (2 przypadki)'


def test_dziennikow():
    """V6.66: dopisz_log / odetnij_niedokonczony_wiersz / write_csv na plikach tymczasowych (12 przypadkow)."""
    import tempfile, contextlib
    global OUT
    stary_out = OUT
    w = []
    DF = lambda **k: pd.DataFrame({x: [v] for x, v in k.items()})
    try:
        with tempfile.TemporaryDirectory() as td, contextlib.redirect_stdout(io.StringIO()):
            T = Path(td)
            txt = lambda p: p.read_text(encoding='utf-8')
            fp = T / 'a.csv'
            dopisz_log(fp, DF(a=1, b=2)); dopisz_log(fp, DF(a=3, b=4))
            w.append(txt(fp) == 'a,b\n1,2\n3,4\n')                                         # 1: zapis + dopisanie
            with open(fp, 'a') as f:
                f.write('5,')
            dopisz_log(fp, DF(a=6, b=7))
            dg = T / 'a.csv.uszkodzone.txt'
            w.append(txt(fp) == 'a,b\n1,2\n3,4\n6,7\n' and '5,\n' in txt(dg))              # 2: przerwany rekord
            with open(fp, 'a') as f:
                f.write('8,"ab')
            dopisz_log(fp, DF(a=9, b='x'))
            w.append(txt(fp).endswith('6,7\n9,x\n') and '8,"ab' in txt(dg))                  # 3: przerwany w polu
            fh_ = T / 'h.csv'; fh_.write_text('a,b')
            dopisz_log(fh_, DF(a=1, b=2))
            w.append(txt(fh_) == 'a,b\n1,2\n')                                               # 4: przerwany naglowek
            dopisz_log(fp, DF(b='y', a=10)); dopisz_log(fp, DF(a=11))
            r = pd.read_csv(fp)
            w.append(r.iloc[-2][['a', 'b']].tolist() == [10, 'y'] and pd.isna(r.iloc[-1]['b']))  # 5: kolejnosc / mniej kolumn
            dopisz_log(fp, DF(a=12, b='z', okazja_id='k1'))
            r = pd.read_csv(fp)
            w.append(list(r.columns) == ['a', 'b', 'okazja_id'] and r.okazja_id.iloc[-1] == 'k1'
                     and (T / 'a.csv.bak').exists())                                        # 6: nowa kolumna
            fn = T / 'n.csv'
            dopisz_log(fn, DF(a=1, b='x\ny'))
            w.append(txt(fn) == 'a,b\n1,x y\n')                                              # 7: nowa linia w polu
            fe = T / 'e.csv'; fe.write_text('a,b\n1,2\n3,')
            (T / 'e.csv.uszkodzone.txt').mkdir()                                            # zapis diagnostyki sie nie uda
            ok = dopisz_log(fe, DF(a=4, b=5))
            w.append(ok is False and txt(fe) == 'a,b\n1,2\n3,')                               # 8: blad diagnostyki
            przed = txt(fp)
            w.append(dopisz_log(fp, pd.DataFrame()) is True and txt(fp) == przed)            # 9: pusty df
            OUT = T
            write_csv('pusty1.csv', [], ('x', 'y'))
            write_csv('pusty2.csv', [], ('x',), cols=['x', 'y', 'z'])
            write_csv('pelny.csv', [{'y': 1, 'x': 2}], ('x',))
            w.append(pd.read_csv(T / 'pusty1.csv').columns.tolist() == ['x', 'y'])          # 10: pusty z 'first'
            w.append(pd.read_csv(T / 'pusty2.csv').columns.tolist() == ['x', 'y', 'z'])     # 11: pusty z 'cols'
            w.append(pd.read_csv(T / 'pelny.csv').columns.tolist() == ['x', 'y'])           # 12: kolejnosc 'first'
    finally:
        OUT = stary_out
    assert all(w), f'test_dziennikow: {w}'
    return f'OK ({len(w)} przypadkow)'


def test_market_luka():
    """V6.58: MARKET z otwarciem za SL/TP1 -> anulowany; normalny MARKET dalej rozliczany."""
    def a5(o, hi, lo, c, n=30):
        a = np.zeros((n, 8))
        a[:, T] = 1_000_000 + np.arange(n) * INTERVAL_MS['5m']
        a[:, O], a[:, H], a[:, L], a[:, C] = c, c * 1.001, c * 0.999, c
        a[0, O], a[0, H], a[0, L], a[0, C] = o, hi, lo, c
        return a
    fees = (6.0, 2.0)
    base = {'entry_type': 'MARKET', 'wait_h': 1, 'setup_type': 'PULLBACK', 'slip_bps': 5}
    w = []
    r = _simulate_core(a5(4.50, 4.52, 4.40, 4.45), 1_000_000, {**base, 'side': 'SHORT', 'entry': 5.142, 'sl': 5.18, 'tp1': 5.0135}, fees)
    w.append(r['status'] == 'anulowany' and not r['filled'])          # 1: short otwarty ponizej TP (BNC 14.10)
    r = _simulate_core(a5(0.95, 0.96, 0.90, 0.91), 1_000_000, {**base, 'side': 'LONG', 'entry': 1.0, 'sl': 0.97, 'tp1': 1.06}, fees)
    w.append(r['status'] == 'anulowany')                               # 2: long otwarty ponizej SL
    r = _simulate_core(a5(1.001, 1.07, 0.995, 1.065), 1_000_000, {**base, 'side': 'LONG', 'entry': 1.0, 'sl': 0.97, 'tp1': 1.06}, fees)
    w.append(r['status'] == 'TP1' and r['r'] > 1.0)                    # 3: normalny MARKET dalej liczony (czesc na 1R)
    assert all(w), f'test_market_luka: {w}'
    return f'OK ({len(w)} przypadki)'


def test_reguly_od():
    """V6.55: data reguly nie przesuwa sie do przodu, gdy archiwum ma nowsze run_utc."""
    import tempfile
    global DRIVE_DIR
    stary = DRIVE_DIR
    try:
        with tempfile.TemporaryDirectory() as td:
            DRIVE_DIR = td
            a1 = pd.DataFrame({'typ_wersja': ['NOWY.v1', 'STARY.v1'], 'run_utc': ['2026-10-23T18:13', '2026-10-01T00:00']})
            od1 = reguly_od(a1, trwale=True)
            assert od1['NOWY.v1'] == '2026-10-23T18:13' and od1['STARY.v1'] == GATE_FROM_UTC, f'test reguly_od 1: {od1}'
            a2 = pd.DataFrame({'typ_wersja': ['NOWY.v1'], 'run_utc': ['2026-10-24T04:51']})   # archiwum nadpisane
            od2 = reguly_od(a2, trwale=True)
            assert od2['NOWY.v1'] == '2026-10-23T18:13', f'test reguly_od 2: {od2}'
            assert reguly_od(a2, trwale=False)['ODBICIE.v1.M'] == REGULY_OD_ZASIEW['ODBICIE.v1.M'], 'test reguly_od 3: zasiew'
    finally:
        DRIVE_DIR = stary
    return 'OK (3 przypadki)'


def jakosc_testu(df, zrodlo, od=None):
    """V6.16: liczniki uczciwosci testu (wszystko + MARKET/LIMIT x LONG/SHORT)."""
    rows = []
    if df is None or not len(df):
        return rows
    df = add_event_ids(df.copy())
    od = od or {}
    df['_po'] = df.snapshot_utc.astype(str).str[:16] >= df.typ_wersja.map(lambda v: od.get(v, '9999'))
    df['_amb'] = _flag(df, 'niejednoznaczne')
    df['_luka'] = _flag(df, 'luka_danych') | (df['status'] == 'luka_danych')
    df['r'] = pd.to_numeric(df['r'], errors='coerce')
    groups = ([('wszystko', df)] + [(f'{e}/{sd}', g) for (e, sd), g in df.groupby(['entry_type', 'side'])]
              + [(f'{tv} {sd}', g) for (tv, sd), g in df.groupby(['typ_wersja', 'side'])])   # V6.21: kazda strategia
    for name, g in groups:
        fin = g[g.status.isin(FINAL)]
        ok = fin[fin.filtr.fillna('') == ''] if 'filtr' in fin.columns else fin
        rows.append({'zrodlo': zrodlo, 'grupa': name, 'wykryte': int(len(g)), 'unikalne_zdarzenia': int(g.event_id.nunique()),
                     'unikalne_okazje': int(g.okazja_id.nunique()), 'rozstrzygniete': int(len(fin)),
                     'niejednoznaczne': int(fin._amb.sum()),
                     'niejednoznaczne_pct': rnd(fin._amb.mean() * 100, 1) if len(fin) else None,
                     'z_luka_danych': int(g._luka.sum()),
                     'po_filtrach_n': int(len(ok)), 'sredni_R_pesymistyczny': rnd(ok.r.mean(), 3) if len(ok) else None,
                     'n_bez_niejednoznacznych': int((~ok._amb).sum()),
                     'sredni_R_bez_niejednoznacznych': rnd(ok[~ok._amb].r.mean(), 3) if (~ok._amb).any() else None,
                     'niezalezne_zdjecia': int(ok.snapshot_utc.nunique()),
                     'unikalne_okazje_po_filtrach': int(ok.okazja_id.nunique()),
                     'po_ustaleniu_regul_n': int(ok._po.sum()),
                     'po_ustaleniu_regul_sredni_R': rnd(ok[ok._po].r.mean(), 3) if ok._po.any() else None,
                     'po_ustaleniu_regul_zdjecia': int(ok[ok._po].snapshot_utc.nunique()),
                     'dane_flag': 'tak' if 'niejednoznaczne' in df.columns and df['niejednoznaczne'].notna().any() else 'brak (stare wiersze)'})
    return rows


_ARCH_BLAD = {}


def zapisz_atomowo(path, zapis, kopia=True):
    """V6.59: zapis do pliku tymczasowego w tym samym katalogu, potem zamiana. kopia=True -> poprzednia wersja jako .bak.
    zapis(sciezka_tymczasowa) wykonuje wlasciwy zapis."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + '.tmp')
    zapis(tmp)
    if tmp.stat().st_size == 0:
        tmp.unlink()
        raise IOError(f'{path.name}: pusty plik po zapisie')
    if kopia and path.exists() and path.stat().st_size > 0:
        shutil.copy2(path, path.with_name(path.name + '.bak'))
    os.replace(tmp, path)


def odetnij_niedokonczony_wiersz(fp):
    """V6.64: dziennik pisany tylko przez ten skrypt (pandas zawsze konczy wiersz nowa linia), a pola tekstowe sa
    oczyszczane z nowych linii -> 1 linia = 1 rekord. Plik bez nowej linii na koncu = przerwany zapis.
    Kolejnosc: 1) fragment za ostatnia nowa linia dopisany do <plik>.uszkodzone.txt (fsync), 2) sprawdzenie, ze
    fragment jest w pliku diagnostycznym, 3) dopiero wtedy obciecie dziennika. Blad w 1-2 -> nic nie obcinane
    (wyjatek). Zwraca liczbe odcietych bajtow (0 = plik w porzadku). Brak nowej linii w calym pliku (zapis przerwany
    jeszcze w naglowku) -> caly plik jest fragmentem, dziennik zostaje pusty i dostanie nowy naglowek."""
    fp = Path(fp)
    if not fp.exists():
        return 0
    size = fp.stat().st_size
    if size == 0:
        return 0
    with open(fp, 'rb') as fh:
        fh.seek(-1, os.SEEK_END)
        if fh.read(1) == b'\n':
            return 0
        pos, cut = size, 0
        while pos > 0:                                  # ostatnia nowa linia, czytane od konca blokami
            start = max(0, pos - 65536)
            fh.seek(start)
            i = fh.read(pos - start).rfind(b'\n')
            if i >= 0:
                cut = start + i + 1
                break
            pos = start
        fh.seek(cut)
        frag = fh.read()
    diag = fp.with_name(fp.name + '.uszkodzone.txt')
    znacznik = f"# {datetime.now(timezone.utc).isoformat(timespec='seconds')} {fp.name}: bajty {cut}-{size} ({len(frag)} B)\n"
    with open(diag, 'ab') as d:
        d.write(znacznik.encode('utf-8') + frag + b'\n')
        d.flush()
        os.fsync(d.fileno())
    with open(diag, 'rb') as d:
        d.seek(-(len(frag) + 1), os.SEEK_END)
        if d.read() != frag + b'\n':
            raise IOError(f'{diag.name}: fragment nie zapisal sie poprawnie - dziennik NIE obciety')
    with open(fp, 'r+b') as fh:
        fh.truncate(cut)
        fh.flush()
        os.fsync(fh.fileno())
    return len(frag)


def dopisz_log(fp, df):
    """V6.63/V6.64: dopisanie wierszy do dziennika CSV (Drive) bez czytania calego pliku w zwyklym przypadku.
    - V6.64: przerwany poprzedni zapis (brak nowej linii na koncu) -> niedokonczony wiersz przeniesiony do
      <plik>.uszkodzone.txt i odciety (odetnij_niedokonczony_wiersz); V6.66: gdy przeniesienie sie nie uda - plik
      bez zmian i NIC nie jest dopisywane (zwraca False; dane tego uruchomienia sa i tak w zipie);
    - V6.64: nowe linie w polach tekstowych zamieniane na spacje (1 linia = 1 rekord);
    - brak pliku / pusty -> zapis atomowy z naglowkiem;
    - nowe kolumny wzgledem naglowka -> plik przepisany raz atomowo (kopia .bak) z suma kolumn;
    - kolumny te same lub mniej / inna kolejnosc -> wiersze dopasowane do naglowka pliku (brakujace puste)."""
    fp = Path(fp)
    if df is None or not len(df):
        return True
    df = df.copy()
    df.columns = [str(c).replace('\r', ' ').replace('\n', ' ') for c in df.columns]
    for c in df.columns:
        if not pd.api.types.is_numeric_dtype(df[c]) and not pd.api.types.is_bool_dtype(df[c]):   # object i string (pandas 3)
            df[c] = df[c].map(lambda v: v.replace('\r', ' ').replace('\n', ' ') if isinstance(v, str) else v)
    if fp.exists():
        try:
            n_b = odetnij_niedokonczony_wiersz(fp)
            if n_b:
                print(f'{fp.name}: poprzedni zapis byl przerwany - niedokonczony wiersz ({n_b} B) przeniesiony do '
                      f'{fp.name}.uszkodzone.txt i odciety')
        except Exception as exc:                        # V6.66: nie dopisuj do uszkodzonego dziennika
            print(f'!!! {fp.name}: nie udalo sie przeniesc niedokonczonego wiersza ({str(exc)[:100]}) - plik bez zmian, '
                  f'wiersze z tego uruchomienia NIE dopisane do tego dziennika (reszta kolektora dziala dalej)')
            return False
    if not fp.exists() or fp.stat().st_size == 0:
        zapisz_atomowo(fp, lambda p: df.to_csv(p, index=False), kopia=False)
        return True
    with open(fp, 'rb') as fh:
        naglowek = fh.readline().decode('utf-8', 'replace').rstrip('\r\n')
    stare = next(csv.reader([naglowek]), [])
    nowe = [c for c in df.columns if c not in stare]
    if nowe or not stare:
        old = pd.read_csv(fp, low_memory=False, on_bad_lines='skip')
        kol = list(dict.fromkeys(list(old.columns) + list(df.columns)))
        full = pd.concat([old, df], ignore_index=True).reindex(columns=kol)
        zapisz_atomowo(fp, lambda p: full.to_csv(p, index=False), kopia=True)
        print(f'{fp.name}: nowe kolumny ({", ".join(nowe[:5])}{" ..." if len(nowe) > 5 else ""}) - '
              f'plik przepisany ({len(old)} + {len(df)} wierszy)')
        return True
    with open(fp, 'a', encoding='utf-8', newline='') as fh:
        df.reindex(columns=stare).to_csv(fh, header=False, index=False)
    return True


def czytaj_csv_z_kopia(path, nazwa):
    """V6.59: odczyt CSV; przy bledzie proba kopii .bak. Zwraca (DataFrame albo None, blad albo None)."""
    path = Path(path)
    bak = path.with_name(path.name + '.bak')
    try:
        return pd.read_csv(path, low_memory=False), None
    except Exception as exc:
        blad = str(exc)[:100]
        print(f'!!! {nazwa}: blad odczytu ({blad})')
    if bak.exists():
        try:
            d = pd.read_csv(bak, low_memory=False)
            print(f'{nazwa}: wczytano kopie zapasowa {bak.name} ({len(d)} wierszy)')
            return d, None
        except Exception as exc:
            print(f'!!! {nazwa}: kopia {bak.name} tez nieczytelna ({str(exc)[:80]})')
    return None, blad


def update_archive(bt, run_utc, drive_ok, now_ms):
    df = pd.DataFrame(bt)
    if len(df):
        df['run_utc'] = run_utc
        df['version'] = VERSION
    path = Path(DRIVE_DIR) / ARCHIVE_NAME
    _ARCH_BLAD.clear()
    if drive_ok and (path.exists() or path.with_name(path.name + '.bak').exists()):
        old, blad = czytaj_csv_z_kopia(path, 'Archiwum') if path.exists() else czytaj_csv_z_kopia(path.with_name(path.name + '.bak'), 'Archiwum (.bak)')
        if old is None:                                  # V6.59: NIE nadpisuj historii niepelnymi danymi
            _ARCH_BLAD['blad'] = blad
            print('!!! Archiwum NIE zostanie zapisane w tym uruchomieniu; handel zablokowany (bramka/ochrona bez historii)')
        else:
            df = pd.concat([old, df], ignore_index=True)
    if not len(df):
        return df
    df = df.sort_values('run_utc', kind='stable')
    df['zone_ref'] = df['zone_ref'].fillna(df['entry']) if 'zone_ref' in df.columns else df['entry']
    df = df.drop_duplicates(subset=['snapshot_utc', 'symbol', 'typ_wersja', 'side', 'zone_ref'], keep='last')
    # ta sama strefa z innego zdjecia (przesuniecie okna testu miedzy uruchomieniami) liczy sie raz - od najwczesniejszego
    df = df.sort_values('snapshot_utc', kind='stable').drop_duplicates(subset=['symbol', 'typ_wersja', 'side', 'zone_ref'], keep='first')
    cutoff = snap_iso(now_ms - BT_ARCHIVE_DAYS * DAY_MS)
    df = df[df.snapshot_utc >= cutoff].sort_values(['snapshot_utc', 'symbol']).reset_index(drop=True)
    df = add_event_ids(df)                                  # V6.16
    if drive_ok and not _ARCH_BLAD:
        try:
            zapisz_atomowo(path, lambda t: df.to_csv(t, index=False))   # V6.59
        except Exception as exc:
            print(f'!!! Archiwum: blad zapisu ({str(exc)[:100]}) - poprzednia wersja zostaje')
    return df


_HIP_BLAD = {}


def hipoteza_mask(df):
    """V6.36: CONT_SWEEP SHORT przy kompresji zmiennosci 1h (krypto). Dziala na DataFrame."""
    if df is None or not len(df):
        return pd.Series(dtype=bool)
    bb = pd.to_numeric(df.get('bb_rank_1h'), errors='coerce')
    sq = df.get('squeeze_1h', pd.Series(False, index=df.index)).astype(str).str.strip().str.lower().isin(['true', '1', '1.0'])
    typ = df['setup_type'] if 'setup_type' in df.columns else df['typ_wersja'].astype(str).str.split('.').str[0]
    coin = df.get('asset_type', pd.Series('COIN', index=df.index)).fillna('COIN') == 'COIN'
    return (typ == 'CONT_SWEEP') & (df['side'] == 'SHORT') & coin & ((bb < HIPOTEZA_BB_RANK_MAX) | sq)


def hipoteza_row(r):
    """V6.36: to samo dla jednego setupu (dict)."""
    bb = r.get('bb_rank_1h')
    sq = str(r.get('squeeze_1h')).strip().lower() in ('true', '1', '1.0')
    return (r.get('setup_type') == 'CONT_SWEEP' and r.get('side') == 'SHORT' and (r.get('asset_type') or 'COIN') == 'COIN'
            and ((bb is not None and not pd.isna(bb) and float(bb) < HIPOTEZA_BB_RANK_MAX) or sq))


def _hip_grupa(dd):
    """V6.38: statystyki jednej grupy hipotezy (wszystko / LIMIT / MARKET)."""
    if dd is None or not len(dd):
        return {'st': {'transakcje': 0}, 'zdjecia': 0, 'okazje': 0, 'pf': {'transakcje': 0}}
    st = stats(dd)
    fin = dd[dd.status.isin(FINAL)]
    pf_d = dd[dd.filtr.fillna('') == ''] if 'filtr' in dd.columns else dd
    return {'st': st, 'zdjecia': st.get('zdjecia') or 0, 'okazje': int(fin.okazja_id.nunique()) if len(fin) else 0,
            'pf': stats(pf_d) if len(pf_d) else {'transakcje': 0}}


def _hip_ok(g, n_min):
    """V6.38: lista brakow grupy (pusta = grupa spelnia wszystkie warunki)."""
    st, braki = g['st'], []
    sr_R, sr_E = progi_R(st)                             # V6.72
    if not (st.get('transakcje', 0) >= n_min and (sr_R if sr_R is not None else -9) >= GATE_MIN_AVG_R
            and (sr_E if sr_E is not None else -9) >= GATE_MIN_EDGE_R and (st.get('t_edge') or -9) >= GATE_MIN_T):
        braki.append('progi')
    if g['zdjecia'] < HIP_MIN_ZDJEC:
        braki.append(f"zdjec {g['zdjecia']} < {HIP_MIN_ZDJEC}")
    if g['okazje'] < HIP_MIN_OKAZJI:
        braki.append(f"okazji {g['okazje']} < {HIP_MIN_OKAZJI}")
    pf_R = progi_R(g['pf'])[0]
    if not ((pf_R if pf_R is not None else -9) > 0):
        braki.append('po filtrach sr <= 0')
    return braki


def hipoteza_table(arch):
    """V6.36-V6.38: hipoteza od HIPOTEZA_OD. Odblokowanie osobno dla LIMIT/MARKET, kazda grupa z pelnym zestawem warunkow."""
    cols = ['hipoteza', 'status', 'status_LIMIT', 'status_MARKET', 'od', 'zdjecia', 'unikalne_okazje', 'transakcje',
            'sredni_R', 'sredni_edge_R', 't_edge', 't', 'skutecznosc_pct', 'suma_R', 'brakuje_transakcji',
            'limit_n', 'limit_R', 'limit_t', 'limit_zdjecia', 'limit_okazje', 'limit_po_filtrach_R',
            'market_n', 'market_R', 'market_t', 'market_zdjecia', 'market_okazje', 'market_po_filtrach_R',
            'po_filtrach_n', 'po_filtrach_R', 'powod']
    d = arch[(arch.snapshot_utc.astype(str) >= HIPOTEZA_OD)] if arch is not None and len(arch) else pd.DataFrame()
    if len(d):
        if 'okazja_id' not in d.columns:
            d = add_event_ids(d.copy())
        d = d[hipoteza_mask(d).values]
    g_all = _hip_grupa(d)
    g = {e: _hip_grupa(d[d.entry_type == e] if len(d) else d) for e in ('LIMIT', 'MARKET')}
    braki_all = _hip_ok(g_all, GATE_MIN_TRADES)
    braki = {e: _hip_ok(g[e], HIP_MIN_N_WEJSCIE) for e in g}
    st_e = {e: ('DO GRY' if not braki_all and not braki[e] else 'OBSERWACJA') for e in g}
    powod = (['lacznie: ' + ', '.join(braki_all)] if braki_all else []) + \
            [f"{e}: {', '.join(braki[e])}" for e in g if braki[e]]
    st = g_all['st']
    x = lambda e, k: g[e]['st'].get(k)
    row = {'hipoteza': 'KOMPRESJA CONT_SWEEP SHORT',
           'status': 'DO GRY' if 'DO GRY' in st_e.values() else 'OBSERWACJA',
           'status_LIMIT': st_e['LIMIT'], 'status_MARKET': st_e['MARKET'], 'od': HIPOTEZA_OD,
           'zdjecia': g_all['zdjecia'], 'unikalne_okazje': g_all['okazje'], 'transakcje': st.get('transakcje', 0),
           'sredni_R': st.get('sredni_R'), 'sredni_edge_R': st.get('sredni_edge_R'), 't_edge': st.get('t_edge'),
           't': st.get('t'), 'skutecznosc_pct': st.get('skutecznosc_pct'), 'suma_R': st.get('suma_R'),
           'brakuje_transakcji': max(0, GATE_MIN_TRADES - st.get('transakcje', 0)),
           'limit_n': x('LIMIT', 'transakcje') or 0, 'limit_R': x('LIMIT', 'sredni_R'), 'limit_t': x('LIMIT', 't_edge'),
           'limit_zdjecia': g['LIMIT']['zdjecia'], 'limit_okazje': g['LIMIT']['okazje'],
           'limit_po_filtrach_R': g['LIMIT']['pf'].get('sredni_R'),
           'market_n': x('MARKET', 'transakcje') or 0, 'market_R': x('MARKET', 'sredni_R'), 'market_t': x('MARKET', 't_edge'),
           'market_zdjecia': g['MARKET']['zdjecia'], 'market_okazje': g['MARKET']['okazje'],
           'market_po_filtrach_R': g['MARKET']['pf'].get('sredni_R'),
           'po_filtrach_n': g_all['pf'].get('transakcje', 0), 'po_filtrach_R': g_all['pf'].get('sredni_R'),
           'powod': '; '.join(powod)}
    return pd.DataFrame([row], columns=cols)


def test_hipotezy():
    """V6.38: syntetyczne archiwum - kazdy rodzaj wejscia zatwierdzany tylko na wlasnych wynikach."""
    global HIPOTEZA_OD
    stare = HIPOTEZA_OD
    HIPOTEZA_OD = '2000-01-01T00:00'
    rng = np.random.default_rng(7)

    def grupa(entry, n, mu, zdjec, filtr='', sym0=0):
        rows = []
        for i in range(n):
            r = mu + rng.normal(0, 0.3)
            rows.append({'snapshot_utc': f'2026-01-{1 + i % zdjec:02d}T{(i % zdjec) % 24:02d}:00', 'symbol': f'S{sym0 + i}USDT',
                         'setup_type': 'CONT_SWEEP', 'typ_wersja': 'CONT_SWEEP.v4' + ('.M' if entry == 'MARKET' else ''),
                         'side': 'SHORT', 'entry_type': entry, 'entry': 1.0 + i * 1e-3, 'zone_ref': 1.0 + i * 1e-3,
                         'status': 'TP1' if r > 0 else 'SL', 'r': r, 'edge_R': r, 'control_R': 0.0, 'filled': True,
                         'filtr': filtr, 'bb_rank_1h': 10.0, 'squeeze_1h': False, 'asset_type': 'COIN'})
        return rows

    def run(rows):
        df = add_event_ids(pd.DataFrame(rows))
        h = hipoteza_table(df).iloc[0]
        return h['status_LIMIT'], h['status_MARKET']

    D, O = 'DO GRY', 'OBSERWACJA'
    przypadki = [
        ('oba dobre', grupa('LIMIT', 60, .5, 25) + grupa('MARKET', 60, .5, 25, sym0=500), (D, D)),
        ('dobry LIMIT, slaby MARKET', grupa('LIMIT', 80, .6, 25) + grupa('MARKET', 40, -.3, 25, sym0=500), (D, O)),
        ('slaby LIMIT, dobry MARKET', grupa('LIMIT', 40, -.3, 25) + grupa('MARKET', 80, .6, 25, sym0=500), (O, D)),
        ('MARKET za malo transakcji', grupa('LIMIT', 60, .5, 25) + grupa('MARKET', 10, .9, 10, sym0=500), (D, O)),
        ('MARKET za malo zdjec', grupa('LIMIT', 60, .5, 25) + grupa('MARKET', 40, .5, 5, sym0=500), (D, O)),
        ('MARKET ujemny po filtrach', grupa('LIMIT', 60, .5, 25) + grupa('MARKET', 30, .6, 25, 'koszt', sym0=500)
         + grupa('MARKET', 30, -.4, 25, sym0=900), (D, O)),
        ('lacznie za malo (oba po 26)', grupa('LIMIT', 24, .5, 24) + grupa('MARKET', 24, .5, 24, sym0=500), (O, O)),
        ('oba slabe', grupa('LIMIT', 60, -.2, 25) + grupa('MARKET', 60, -.2, 25, sym0=500), (O, O)),
    ]
    try:
        for nazwa, rows, oczek in przypadki:
            wynik = run(rows)
            assert wynik == oczek, f'test hipotezy "{nazwa}": {wynik} != {oczek}'
    finally:
        HIPOTEZA_OD = stare
    return f'OK ({len(przypadki)} przypadkow)'


def apply_hipoteza(live, hip):
    """V6.36/V6.37: etykieta hipotezy; 'DO GRY' tylko dla rodzaju wejscia, ktory sam spelnia progi (reszta blokad bez zmian)."""
    h = hip.iloc[0].to_dict() if hip is not None and len(hip) else {}
    for r in live:
        if not hipoteza_row(r):
            r['hipoteza'] = r['hip_n'] = r['hip_R'] = r['hip_status'] = None
            continue
        st_e = h.get(f"status_{r.get('entry_type')}") or 'OBSERWACJA'
        r['hipoteza'], r['hip_n'], r['hip_R'], r['hip_status'] = 'KOMPRESJA', h.get('transakcje'), h.get('sredni_R'), st_e
        if st_e == 'DO GRY':
            r['status_bramki'] = 'DO GRY'


def gate_table(arch, now_ms, od=None):
    """V6.17: bramka per typ_wersja x kierunek, tylko zdjecia po ustaleniu regul (od[typ_wersja])."""
    rows = []
    od = od or {}
    base = max(GATE_FROM_UTC, snap_iso(now_ms - GATE_WINDOW_DAYS * DAY_MS))
    combos = [(t, f'{t}.v{v}') for t, v in SETUP_TYPES.items()]
    if MARKET_ENTRIES:
        combos += [(t, f'{t}.v{v}.M') for t, v in SETUP_TYPES.items()]
    if len(arch) and 'okazja_id' not in arch.columns:
        arch = add_event_ids(arch.copy())
    for (typ, tv), side in [(c, sd) for c in combos for sd in ('LONG', 'SHORT')]:
        since = max(base, od_strona(od, tv, side, base))
        d = arch[(arch.typ_wersja == tv) & (arch.side == side) & (arch.snapshot_utc >= since)] if len(arch) else arch
        ok = d[d.filtr.fillna('') == ''] if len(d) else d
        st = stats(ok) if len(ok) else {'transakcje': 0}
        st_all = stats(d) if len(d) else {'transakcje': 0}
        fin = ok[ok.status.isin(FINAL)] if len(ok) else ok
        sr_R, sr_E = progi_R(st)                         # V6.72: waga zdjecia rynku
        active = (st.get('transakcje', 0) >= GATE_MIN_TRADES and (sr_R if sr_R is not None else -9) >= GATE_MIN_AVG_R
                  and (sr_E if sr_E is not None else -9) >= GATE_MIN_EDGE_R and (st.get('t_edge') or -9) >= GATE_MIN_T)
        rows.append({'typ': tv, 'side': side, 'status': 'DO GRY' if active else 'OBSERWACJA', 'od': since,
                     'unikalne_okazje': int(fin.okazja_id.nunique()) if len(fin) else 0,
                     'transakcje': st.get('transakcje', 0), 'sredni_R': sr_R,
                     'sredni_edge_R': sr_E, 't_edge': st.get('t_edge'), 't': st.get('t'),
                     'sredni_R_transakcje': st.get('sredni_R'), 'sredni_edge_R_transakcje': st.get('sredni_edge_R'),
                     'skutecznosc_pct': st.get('skutecznosc_pct'), 'zdjecia': st.get('zdjecia'),
                     'brakuje_transakcji': max(0, GATE_MIN_TRADES - st.get('transakcje', 0)),
                     'bez_filtrow_transakcje': st_all.get('transakcje', 0), 'bez_filtrow_sredni_R': st_all.get('sredni_R'),
                     'bez_filtrow_t': st_all.get('t')})
    return pd.DataFrame(rows)


def bt_summary(df):
    if not len(df):
        return {}
    out = {'WSZYSTKIE (po filtrach)': stats(df[df.filtr.fillna('') == ''])}
    out['WSZYSTKIE (bez filtrow)'] = stats(df)
    for typ in sorted(df.setup_type.unique()):
        d = df[df.setup_type == typ]
        out[f'{typ} po filtrach'] = stats(d[d.filtr.fillna('') == ''])
        for sd in ('LONG', 'SHORT'):                     # V6.30: bez mieszania kierunkow
            x = d[(d.side == sd) & (d.filtr.fillna('') != '')]
            if len(x):
                out[f'{typ} {sd} odrzucone'] = stats(x)
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


def short_history_path():
    return Path(DRIVE_DIR) / 'short_history_v6110.json'


def load_short_history(drive_ok):
    _short_history_verified.clear()
    if not drive_ok:
        return
    try:
        records = json.loads(short_history_path().read_text(encoding='utf-8'))
        if not isinstance(records, dict):
            return
        for key, value in records.items():
            if isinstance(value, dict) and isinstance(value.get('earliest_ms'), int):
                sym, sep, tf = key.partition('|')
                if sep and tf in INTERVAL_MS:
                    _short_history_verified[(sym, tf)] = value['earliest_ms']
    except (OSError, ValueError, TypeError):
        pass  # brak dowodu -> pelny odczyt; nie ryzykujemy pominiecia historii


def save_short_history(drive_ok):
    if not drive_ok:
        return
    # Zachowujemy tylko potwierdzenia z ostatniego pelnego pobrania.
    records = {f'{sym}|{tf}': {'earliest_ms': earliest}
               for (sym, tf), earliest in _short_history_verified.items()}
    for (sym, tf), info in _history_repairs.items():
        if info.get('blad'):
            continue
        if info.get('krotka_historia'):
            records[f'{sym}|{tf}'] = {'earliest_ms': info['pierwsza_swieca']}
        elif info.get('powod', '').startswith('pelne') and info.get('otrzymano', 0) >= INTERVALS[tf]:
            records.pop(f'{sym}|{tf}', None)
    path = short_history_path()
    tmp = path.with_suffix('.tmp')
    try:
        tmp.write_text(json.dumps(records, ensure_ascii=False), encoding='utf-8')
        os.replace(tmp, path)
    except OSError as exc:
        print(f'Potwierdzenia krotkiej historii: blad zapisu ({str(exc)[:100]})')
    finally:
        try:
            tmp.unlink(missing_ok=True)
        except OSError:
            pass


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
                for tf0, arr0 in arrs.items():
                    if tf0 in INTERVAL_MS:
                        _record_zeros(sym, tf0, 'ARCHIWUM', arr0, from_array=True)
                _arch_cache[sym] = arrs
                known[sym] = {}
                for tf, a in arrs.items():
                    if not len(a) or tf not in INTERVAL_MS:
                        continue
                    # Sprawdz ostatnie 100 swiec. Starsze luki zostaja w QC;
                    # naprawa calosci wymagalaby stronicowania i niepotrzebnego API.
                    tail = np.asarray(a[-100:, T], dtype=np.int64)
                    contiguous = bool(np.all(np.diff(tail) == INTERVAL_MS[tf])) if len(tail) > 1 else True
                    if not contiguous:
                        _fetch_count(f'archiwum_{tf}_luka')
                    known[sym][tf] = (int(a[0, T]), int(a[-1, T]), contiguous,
                                      _short_history_verified.get((sym, tf)) == int(a[0, T]))
    return known


FUNDING_CACHE = 'funding_cache_v69.json'


def load_funding_cache(drive_ok):
    """V6.107: cache fundingu na Drive z odzyskiwaniem kopii i jawna diagnostyka.
    Uszkodzony plik nie moze wylaczyc pobrania nowych stawek; brak cache -> pelne API.
    """
    p = Path(DRIVE_DIR) / FUNDING_CACHE
    _funding_io.update(odczyt='wylaczony', wpisy=0, aktualne=0, zapis='nie_wykonano')
    if not drive_ok:
        return {}
    for file, label in ((p, 'plik'), (p.with_suffix(p.suffix + '.bak'), 'kopia')):
        if not file.exists():
            continue
        try:
            x = json.loads(file.read_text(encoding='utf-8'))
            if not isinstance(x, dict):
                raise ValueError('niepoprawny format')
            clean = {sym: hist for sym, hist in x.items() if isinstance(hist, list) and len(hist) >= 8}
            _funding_io.update(odczyt=label, wpisy=len(clean))
            if label == 'kopia':
                print(f'  funding cache: odzyskano kopie ({len(clean)} kontraktow)')
            return clean
        except (OSError, ValueError, TypeError) as exc:
            _funding_io['odczyt'] = 'blad: ' + str(exc)[:80]
    if not p.exists():
        _funding_io['odczyt'] = 'brak_pliku'
    return {}


def save_funding_cache(coins, old, drive_ok):
    """Atomiczny zapis z walidacja i kopia; stare kompletne serie nie znikaja przy bledzie API."""
    if not drive_ok:
        return
    new = dict(old or {})
    for sym, d in coins.items():
        h = d.get('funding_history')
        if isinstance(h, list) and len(h) >= 8:
            new[sym] = h
    p = Path(DRIVE_DIR) / FUNDING_CACHE
    temp = p.with_suffix(p.suffix + '.tmp')
    bak = p.with_suffix(p.suffix + '.bak')
    try:
        encoded = json.dumps(new, separators=(',', ':'), ensure_ascii=False)
        temp.write_text(encoded, encoding='utf-8')
        # Walidacja przed podmiana, bez wywolywania zewnetrznego kodu.
        if not isinstance(json.loads(temp.read_text(encoding='utf-8')), dict):
            raise ValueError('blad zapisu cache fundingu')
        if p.exists():
            import shutil as _shutil
            _shutil.copy2(p, bak)
        os.replace(temp, p)
        _funding_io.update(zapis='ok', wpisy_po_zapisie=len(new), rozmiar_b=len(encoded.encode('utf-8')))
    except Exception as exc:
        _funding_io['zapis'] = 'blad: ' + str(exc)[:100]
        print(f'Pamiec fundingu: blad zapisu ({str(exc)[:100]})')
    finally:
        try:
            if temp.exists():
                temp.unlink()
        except OSError:
            pass


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
                info = _history_repairs.get((sym, tf))
                if info is not None and info.get('powod') != 'archiwum_aktualne':
                    gap_before = int(np.sum(np.diff(old[tf][-100:, T]) > INTERVAL_MS[tf] * 1.5)) if tf in old and len(old[tf]) > 1 else 0
                    gap_after = int(np.sum(np.diff(m[-100:, T]) > INTERVAL_MS[tf] * 1.5)) if len(m) > 1 else 0
                    info['luki_przed'] = gap_before
                    info['luki_po'] = gap_after
                    info['naprawa_wynik'] = ('POPRAWA' if gap_after < gap_before else
                                             'BRAK_POPRAWY' if gap_before and gap_after >= gap_before else
                                             'BEZ_LUK_W_OGONIE')
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


BRAK_DYSKU_TXT = ('!!! BRAK GOOGLE DRIVE - WYNIKI NIEWIARYGODNE !!!\n'
                  'Bez Dysku nie ma archiwum, bramki, dziennika PROBA ani historii OI: bramka liczy od zera (nic nie przejdzie\n'
                  'do gry), tryb ochronny i hipoteza licza sie z jednego przebiegu, proby NIE beda rozliczane.\n'
                  'Uruchom komorke z drive.mount("/content/drive"), zaloguj sie i uruchom skrypt ponownie.')


def baner_brak_dysku():
    """V6.42: glosne ostrzezenie o braku Google Drive."""
    print('\n' + '!' * 90 + '\n' + BRAK_DYSKU_TXT + '\n' + '!' * 90 + '\n')


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
MOVERS_N = 15                     # V6.62: ile wzrostow i ile spadkow 24h w movers.csv
MOVERS_COLS = ('grupa', 'symbol', 'chg_24h_pct', 'chg_4h_pct', 'ext_ema50_atr_1h', 'rsi_1h', 'regime_1h', 'quote_vol_24h',
               'vol_rank', 'tradable', 'nie_do_handlu', 'setupy', 'odrzucone_przy_budowie')   # V6.66: tez naglowek pustego pliku
SUMMARY_FIRST = ('symbol', 'live_price', 'tradable', 'not_tradable_reason', 'asset_type', 'quote_vol_24h')   # V6.66


def movers_rows(summary, live, odrz):
    """V6.62: top MOVERS_N wzrostow i spadkow 24h: czy do handlu, jakie setupy (ocena, warunki), co odrzucono przy budowie."""
    sm = pd.DataFrame(summary)
    if not len(sm) or 'chg_24h_pct' not in sm.columns:
        return []
    sm = sm[pd.to_numeric(sm['chg_24h_pct'], errors='coerce').notna()].copy()
    sm['chg_24h_pct'] = sm['chg_24h_pct'].astype(float)
    up = sm.nlargest(MOVERS_N, 'chg_24h_pct').assign(grupa='wzrost')
    dn = sm.nsmallest(MOVERS_N, 'chg_24h_pct').assign(grupa='spadek')
    odz = {}
    if odrz is not None and len(odrz):
        # V6.172: bez deprecated DataFrameGroupBy.apply na kolumnach grupujacych.
        for sym, group in odrz.groupby('symbol', sort=False):
            odz[sym] = '; '.join(dict.fromkeys(
                f"{a} {b} {c}: {reason}" for a, b, c, reason in zip(
                    group['side'], group['setup_type'], group['entry_type'], group['powod'])))
    st = {}
    for r in live:
        st.setdefault(r['symbol'], []).append(
            f"[{r.get('ocena', '?')}] {r['side']} {r['setup_type']} {r['entry_type']}: {r.get('warunki') or r.get('filtr') or 'OK'}")
    rows = []
    for r in pd.concat([up, dn]).to_dict('records'):
        rows.append({'grupa': r['grupa'], 'symbol': r['symbol'], 'chg_24h_pct': r['chg_24h_pct'],
                     'chg_4h_pct': r.get('chg_4h_pct'), 'ext_ema50_atr_1h': r.get('ext_ema50_atr_1h'),
                     'rsi_1h': r.get('rsi_1h'), 'regime_1h': r.get('regime_1h'), 'quote_vol_24h': r.get('quote_vol_24h'),
                     'vol_rank': r.get('vol_rank'), 'tradable': r.get('tradable'),
                     'nie_do_handlu': r.get('not_tradable_reason') or '',
                     'setupy': ' | '.join(st.get(r['symbol'], [])) or 'brak',
                     'odrzucone_przy_budowie': odz.get(r['symbol'], '')})
    return rows


def write_csv(name, rows, first=(), cols=None):
    """V6.65: pusty plik zawsze z naglowkiem - kolumny z 'cols' (pelny schemat, np. z setups_all.csv),
    inaczej z 'first'. Wczesniej pusty plik nie mial nawet naglowka (pd.read_csv: EmptyDataError)."""
    df = pd.DataFrame(rows)
    if len(df):
        cols_ = list(dict.fromkeys([c for c in first if c in df.columns] + [c for c in df.columns if c not in first]))
        df = df[cols_]
    else:
        df = pd.DataFrame(columns=list(dict.fromkeys(list(cols) if cols is not None and len(cols) else list(first))))
    # V6.171: nie powielaj naglowkow takze dla pustych tabel i list cols/first.
    df, _ = schemat_bez_duplikatow_v170(df, name)
    df.to_csv(OUT / name, index=False)
    return df


SETUP_FIRST = ['ocena', 'status_setupu', 'blokady_bezpieczenstwa', 'blokady_kwalifikacji', 'ostrzezenia_tradfi', 'blokady_wykonania_tradfi', 'pewnosc_przewagi', 'po_reg_okazje', 'ocena_pkt', 'status_bramki', 'symbol', 'side', 'setup_type', 'entry_type', 'price', 'entry', 'dist_entry_pct', 'sl',
               'tp1', 'tp2', 'rr_tp1_net', 'risk_pct', 'dzwignia', 'dzwignia_bezp', 'dzwignia_pref', 'likwidacja', 'likw_vs_sl', 'wartosc_usdt', 'depozyt_usdt', 'depozyt_pct', 'ilosc', 'strata_sl_usdt', 'zysk_tp1_usdt', 'dzwignia_uwagi', 'cost_R', 'sl_atr', 'wait_h', 'score_R', 'hist_n', 'hist_sr_R',
               'hist_edge_R', 'hist_t_edge', 'hist_zrodlo', 'tp_liq', 'rr_tp_liq', 'liq_tp_share', 'liq_za_sl_share',
               'trigger', 'cancel_if', 'wazne_do_pl', 'retesty_15m', 'ruch24_kier', 'kontra_24h', 'pogon_24h', 'wolumen_skok', 'filtr', 'tradable', 'not_tradable_reason', 'typ_transakcje', 'typ_sredni_R', 'typ_t']
TOP_FIRST = ['mode', 'rank', 'status_bramki', 'symbol', 'side', 'setup_type', 'price', 'entry', 'dist_entry_pct', 'sl', 'tp1',
             'tp2', 'rr_tp1_net', 'risk_pct', 'dzwignia', 'dzwignia_bezp', 'dzwignia_pref', 'likwidacja', 'likw_vs_sl', 'wartosc_usdt', 'depozyt_usdt', 'depozyt_pct', 'ilosc', 'strata_sl_usdt', 'zysk_tp1_usdt', 'dzwignia_uwagi', 'cost_R', 'score_R', 'dlaczego', 'trigger', 'cancel_if']
RANK_FIRST = ['ocena', 'status_setupu', 'blokady_bezpieczenstwa', 'blokady_kwalifikacji', 'ostrzezenia_tradfi', 'blokady_wykonania_tradfi', 'pewnosc_przewagi', 'po_reg_okazje', 'ocena_pkt', 'do_handlu', 'w_limicie_ryzyka', 'warunki', 'entry_type', 'symbol', 'vol_rank', 'asset_type', 'side', 'setup_type', 'price', 'entry', 'dist_entry_pct', 'sl', 'tp1',
              'rr_tp1_net', 'risk_pct', 'cost_R', 'dzwignia', 'dzwignia_bezp', 'filtr', 'przegrzanie', 'zgodnosc_momentum', 'ocena_powody', 'trigger', 'cancel_if', 'wazne_do_pl', 'retesty_15m']
POS_FIRST = ['symbol', 'side', 'akcja', 'zamknij_pct', 'nowy_sl', 'zamknij_najpozniej_pl', 'price', 'entry', 'sl', 'tp', 'R_teraz', 'MFE_R', 'pnl_usdt',
             'powody', 'uwagi']
BT_FIRST = ['snapshot_utc', 'symbol', 'setup_type', 'typ_wersja', 'side', 'entry_type', 'entry', 'sl', 'tp1',
            'rr_tp1_net', 'filtr', 'status', 'filled', 'r', 'control_R', 'edge_R', 'tp_liq', 'status_liq', 'r_liq', 'mfe_R', 'mae_R', 'fill_utc', 'exit_utc', 'bars_held']



# =================== V6.76: ELLIOTT - RAPORTY BADAWCZE ===================
ELLIOTT_GROUPS = ('BRAK', 'ZGODNY_Z_ROZWIJAJACYM', 'PRZECIW_ROZWIJAJACEMU',
                  'ZGODNY_Z_ZAKONCZONYM', 'PO_IMPULSIE', 'NIEOBLICZONE')


# V6.170: audyt schematu raportowego, bez zmiany strategii / bramek.
def schemat_bez_duplikatow_v170(frame, nazwa='raport'):
    """Laczy wylacznie IDENTYCZNE kolumny o tej samej nazwie.
    W razie roznych wartosci fail-closed: nie wolno cicho pominac danych.
    Zwraca (dataframe, rekordy audytu).
    """
    if frame is None or not isinstance(frame, pd.DataFrame):
        return frame, []
    labels=list(frame.columns)
    repeated=list(dict.fromkeys(k for k in labels if labels.count(k)>1))
    out=frame
    log=[]
    for label in repeated:
        indices=[i for i,k in enumerate(labels) if k==label]
        base=frame.iloc[:,indices[0]]
        for i in indices[1:]:
            other=frame.iloc[:,i]
            if not base.equals(other):
                # Ignoruj tylko roznice reprezentacji NA, nigdy rozne wartosci.
                raise ValueError(f'V6.170: konflikt powtorzonej kolumny {nazwa}/{label} na indeksach {indices}; raport niezapisany')
        log.append({'raport':nazwa,'kolumna':str(label),'liczba_wystapien':len(indices),
                    'usuniete_kopie':len(indices)-1,'status':'IDENTYCZNE_SCALONE'})
    if repeated:
        out=frame.iloc[:,[i for i,k in enumerate(labels) if k not in repeated or i==labels.index(k)]].copy()
        assert out.columns.is_unique
        assert len(out)==len(frame)
    return out,log



# V6.174: bezpieczna konwersja audytowych ramek do rekordow.
# Zrodlem ostrzezen byly posrednie DF, nie finalne pliki CSV.
_duplikaty_rekordow_v174 = []
def rekordy_audytu_v174(frame, zrodlo):
    clean, log = schemat_bez_duplikatow_v170(frame, zrodlo)
    if log:
        _duplikaty_rekordow_v174.extend(log)
    return clean.to_dict('records')

def test_v174():
    x=pd.DataFrame([[1,1,2],[3,3,4]], columns=['exit_utc','exit_utc','r_netto'])
    original=len(_duplikaty_rekordow_v174)
    rows=rekordy_audytu_v174(x, 'test_v174')
    assert rows==[{'exit_utc':1,'r_netto':2},{'exit_utc':3,'r_netto':4}]
    assert len(_duplikaty_rekordow_v174)==original+1
    del _duplikaty_rekordow_v174[original:]
    bad=pd.DataFrame([[1,2]],columns=['exit_utc','exit_utc'])
    try: rekordy_audytu_v174(bad,'test_v174_konflikt')
    except ValueError: pass
    else: raise AssertionError('V174: sprzeczne dane nie moga przejsc')
    assert rekordy_audytu_v174(pd.DataFrame(columns=['a','a']), 'empty')==[]
    del _duplikaty_rekordow_v174[original:]
    return 'PASS (duplikaty wewnetrzne, konflikt fail-closed, puste DF)'

def test_v170():
    sample=pd.DataFrame([[1,2,1],[3,4,3]],columns=['a','b','a'])
    clean,log=schemat_bez_duplikatow_v170(sample,'test')
    assert list(clean.columns)==['a','b'] and clean['a'].tolist()==[1,3]
    assert len(log)==1 and log[0]['usuniete_kopie']==1
    try:
        schemat_bez_duplikatow_v170(pd.DataFrame([[1,2]],columns=['a','a']),'test')
    except ValueError:
        pass
    else:
        raise AssertionError('V170: konflikt musi zatrzymac zapis')
    clean,log=schemat_bez_duplikatow_v170(pd.DataFrame({'a':[1]}),'test')
    assert not log and clean['a'].tolist()==[1]
    return 'PASS (identyczne kopie, konflikt fail-closed, bez zmian danych)'



# V6.171: audyt finalnych artefaktow CSV PRZED tworzeniem ZIP; weryfikuje rowniez puste CSV
# i pliki zapisane poza write_csv(). Przy powtorzonym naglowku scala tylko kolumny
# o identycznej zawartosci; sprzecznosc = fail-closed, z zachowaniem oryginalu.
def csv_finalny_audyt_v171(root):
    import csv
    from collections import Counter
    root = Path(root)
    audit = []
    for file in sorted(root.rglob('*.csv')):
        if file.name == 'kolumny_eksport_audyt_v171.csv':
            continue
        with file.open('r', newline='', encoding='utf-8-sig') as fh:
            reader = csv.reader(fh)
            head = next(reader, None)
            if head is None:
                audit.append({'plik':str(file.relative_to(root)),'status':'PUSTY_PLIK_BEZ_NAGLOWKA','usuniete_kolumny':0,'wiersze':0})
                continue
            dup = {k:v for k,v in Counter(head).items() if v > 1}
            if not dup:
                audit.append({'plik':str(file.relative_to(root)),'status':'OK','usuniete_kolumny':0,'wiersze':None})
                continue
            # Wszystkie wiersze sa sprawdzane; zadnego wyboru na podstawie pierwszego rekordu.
            keep = [i for i,k in enumerate(head) if head.index(k)==i]
            repeats = [(head.index(k),i,k) for i,k in enumerate(head) if head.index(k)!=i]
            rows = []
            for nr,row in enumerate(reader, start=2):
                if len(row)!=len(head):
                    raise ValueError(f'V6.171: rozna liczba pol {file.name}, wiersz {nr}: {len(row)} != {len(head)}')
                for a,b,k in repeats:
                    if row[a] != row[b]:
                        raise ValueError(f'V6.171: SPRZECZNE kolumny {file.name}/{k}, wiersz {nr}, indeksy {a},{b}; zachowano oryginal')
                rows.append([row[i] for i in keep])
        # Zapis atomowy dopiero po sprawdzeniu calego pliku.
        temp = file.with_name(file.name+'.v171tmp')
        try:
            with temp.open('w', newline='', encoding='utf-8') as fh:
                wr=csv.writer(fh)
                wr.writerow([head[i] for i in keep])
                wr.writerows(rows)
            temp.replace(file)
        finally:
            if temp.exists(): temp.unlink()
        audit.append({'plik':str(file.relative_to(root)),'status':'IDENTYCZNE_SCALONE',
                      'usuniete_kolumny':len(head)-len(keep),'wiersze':len(rows),
                      'kolumny':'; '.join(dup)})
    # Niezalezny drugi odczyt wszystkich naglowkow.
    for file in root.rglob('*.csv'):
        with file.open('r', newline='', encoding='utf-8-sig') as fh:
            head=next(csv.reader(fh),None)
        if head is not None and len(set(head)) != len(head):
            raise AssertionError(f'V6.171: duplikat naglowka po eksporcie: {file.name}')
    with (root/'kolumny_eksport_audyt_v171.csv').open('w',newline='',encoding='utf-8') as fh:
        wr=csv.DictWriter(fh, fieldnames=['plik','status','usuniete_kolumny','wiersze','kolumny'],extrasaction='ignore')
        wr.writeheader(); wr.writerows(audit)
    return audit


def test_v171():
    import tempfile, csv
    with tempfile.TemporaryDirectory() as td:
        p=Path(td)
        (p/'pusty.csv').write_text('a,b,a\n',encoding='utf-8')
        (p/'dane.csv').write_text('a,b,a\n1,2,1\n3,4,3\n',encoding='utf-8')
        a=csv_finalny_audyt_v171(p)
        assert len(a)==2 and (p/'pusty.csv').read_text().strip()=='a,b'
        assert (p/'dane.csv').read_text().splitlines()[1]=='1,2'
        (p/'konflikt.csv').write_text('a,a\n1,2\n',encoding='utf-8')
        try: csv_finalny_audyt_v171(p)
        except ValueError: pass
        else: raise AssertionError('V171: konflikt nie zostal wykryty')
        assert (p/'konflikt.csv').read_text()=='a,a\n1,2\n'
    return 'PASS (puste CSV, wszystkie wiersze, konflikt bez utraty pliku)'

def elliott_tylko_po_filtrach(df):
    """V6.76 (poprawka): raporty Elliotta na tych samych setupach co bramka - pusty filtr (w tym bez typ_wylaczony).
    Bez tego wylaczone typy (PULLBACK LONG, SWING4H...) i odfiltrowane setupy mieszaly sie z grupami."""
    if df is None or not len(df) or 'filtr' not in df.columns:
        return df
    return df[df['filtr'].fillna('').astype(str) == '']


def elliott_oznaczenie(df):
    """Stare wiersze z nieobliczonym Elliott != obliczona struktura BRAK.
    Nigdy nie uzupełniaj historii analizą z dzisiejszych świec.
    """
    if 'elliott_vs_setup' not in df.columns:
        return pd.Series('NIEOBLICZONE', index=df.index)
    x = df['elliott_vs_setup'].astype('string').str.strip()
    x = x.where(x.isin(ELLIOTT_GROUPS[:-1]), 'NIEOBLICZONE')
    return x.fillna('NIEOBLICZONE').astype(str)


def elliott_statystyki(df):
    cols = ['grupa', 'setup_type', 'side', 'entry_type', 'transakcje', 'okazje',
            'zdjecia', 'sredni_R_netto', 'sredni_R_zdjec', 'win_rate_pct',
            'suma_R', 'uwaga']
    if df is None or df.empty or 'status' not in df.columns:
        return pd.DataFrame(columns=cols)
    x = df.copy()
    x['grupa'] = elliott_oznaczenie(x)
    x = x[x['status'].isin(FINAL)].copy()
    if not len(x):
        return pd.DataFrame(columns=cols)
    x['r'] = pd.to_numeric(x['r'], errors='coerce')
    x = x[x['r'].notna() & np.isfinite(x['r'])]
    if not len(x):
        return pd.DataFrame(columns=cols)
    if 'okazja_id' not in x:
        x = add_event_ids(x)
    rows=[]
    for keys in [('grupa',), ('grupa','setup_type','side','entry_type')]:
        for key, g in x.groupby(list(keys), dropna=False):
            if not isinstance(key, tuple): key=(key,)
            d=dict(zip(keys,key))
            per=g.groupby('snapshot_utc')['r'].mean() if 'snapshot_utc' in g else pd.Series(dtype=float)
            rows.append({**{v:d.get(v,'WSZYSTKIE') for v in cols[:4]},
                         'transakcje':int(len(g)), 'okazje':int(g['okazja_id'].nunique()),
                         'zdjecia':int(per.size),
                         'sredni_R_netto':round(float(g['r'].mean()),4),
                         'sredni_R_zdjec':round(float(per.mean()),4) if len(per) else None,
                         'win_rate_pct':round(float((g['r']>0).mean()*100),2),
                         'suma_R':round(float(g['r'].sum()),3),
                         'uwaga':'historyczny wynik, nie walidacja OOS; rozliczone tylko'})
    return pd.DataFrame(rows,columns=cols)


def elliott_oos_jakosc(df, min_okazje=20, min_zdjec=10):
    """Opisowa kontrola OOS, bez uzywania danych do decyzji handlowych.
    Dla grupy fazy wymagana jest niezaleznosc sygnalow i zdjec.
    """
    cols=['grupa','transakcje','okazje','zdjecia','sredni_R_netto',
          'min_okazje','min_zdjec','status_proby','uwaga']
    stats=elliott_statystyki(df)
    if stats.empty:
        return pd.DataFrame(columns=cols)
    x=stats[(stats['setup_type']=='WSZYSTKIE') & (stats['side']=='WSZYSTKIE')].copy()
    x=x[['grupa','transakcje','okazje','zdjecia','sredni_R_netto']]
    x['min_okazje']=int(min_okazje)
    x['min_zdjec']=int(min_zdjec)
    x['status_proby']=np.where((x['okazje']>=min_okazje)&(x['zdjecia']>=min_zdjec),
                                'MINIMUM_ILOŚCIOWE','NIEWYSTARCZAJACA_PROBA')
    x['uwaga']='Minimum ilosciowe nie oznacza statystycznego potwierdzenia przewagi ani zgody na handel'
    return x[cols]


def elliott_portfel_ab(df, miejsca=3, return_weekly=False, return_exit_weekly=False, extra_cost_R=0.0, tie_reverse=False,
                       warianty=('BAZA','ELLIOTT_HIPOTEZA'), asof_utc=None, audit_trace=False):
    """Diagnostyczne A/B: wybiera WSZYSTKIE kandydatury w czasie zdjecia, bez
    znajomosci statusu, wypelnienia i wyniku. Wynik z symulacji jest dolaczany
    dopiero po wyborze; oczekujacy LIMIT rezerwuje miejsce do fill/anulowania/TTL.
    Nie odtwarza calosci realnych blokad, depozytu ani poslizgu faktycznej egzekucji.
    """
    elliott_portfel_ab._last_trade_events = []
    cols=['wariant','zlecenia_wybrane','transakcje','niewypelnione','anulowane','w_toku',
          'zdjecia','suma_R','sredni_R_netto','win_rate_pct',
          'maks_aktywne_LONG','maks_aktywne_SHORT','brak_fill_utc','max_drawdown_R','uwaga']
    required={'snapshot_utc','symbol','side','status','r'}
    if df is None or df.empty or not required.issubset(df.columns):
        return (pd.DataFrame(columns=cols), pd.DataFrame()) if return_weekly else pd.DataFrame(columns=cols)
    x=df.copy()
    # As-of jest opcjonalne; stosowane dla niezaleznego benchmarku OOS.
    asof=pd.to_datetime(asof_utc,utc=True,errors='coerce') if asof_utc is not None else pd.NaT
    x['grupa']=elliott_oznaczenie(x)
    for c in ['snapshot_utc','fill_utc','exit_utc']:
        if c not in x: x[c]=None
        x[c+'_dt']=pd.to_datetime(x[c],utc=True,errors='coerce')
    x=x[x['snapshot_utc_dt'].notna() & x['side'].isin(['LONG','SHORT'])].copy()
    if pd.notna(asof):
        x=x.loc[x['snapshot_utc_dt']<=asof].copy()
        # Pozniejsze zamkniecia sa jeszcze nierozliczone; fill po asof nie jest znany.
        future_exit=x['exit_utc_dt'].isna() | (x['exit_utc_dt']>asof)
        x.loc[future_exit,'status']=np.where(x.loc[future_exit,'entry_type'].astype(str).eq('LIMIT') &
            (x.loc[future_exit,'fill_utc_dt'].isna() | (x.loc[future_exit,'fill_utc_dt']>asof)), 'oczekuje','otwarty') if 'entry_type' in x else 'otwarty'
        x.loc[future_exit,'r']=np.nan
        x.loc[future_exit,'exit_utc_dt']=pd.NaT
        x.loc[x['fill_utc_dt']>asof,'fill_utc_dt']=pd.NaT
    if x.empty: return (pd.DataFrame(columns=cols), pd.DataFrame()) if return_weekly else pd.DataFrame(columns=cols)
    if 'event_id' not in x:
        if {'entry','setup_type','typ_wersja','entry_type'}.issubset(x.columns):
            x=add_event_ids(x)
        else:
            x['event_id']=x.index.astype(str)
    x['r']=pd.to_numeric(x['r'],errors='coerce')
    for c in ['score_R','rr_tp1_net','cost_R','wait_h']:
        x[c]=pd.to_numeric(x[c],errors='coerce') if c in x else np.nan
    if 'entry_type' not in x: x['entry_type']='MARKET'
    # setup_type is optional in historical regression fixtures; preserve the event schema.
    if 'setup_type' not in x: x['setup_type']=''
    pri={'ZGODNY_Z_ROZWIJAJACYM':0, 'PO_IMPULSIE':1,
         'ZGODNY_Z_ZAKONCZONYM':2, 'PRZECIW_ROZWIJAJACEMU':3,
         'BRAK':4, 'NIEOBLICZONE':5}
    x['_ell']=x['grupa'].map(pri).fillna(6)                                   # V6.97: raz, nie per zdjecie
    _POTRZEBNE=['snapshot_utc_dt','snapshot_utc','symbol','side','event_id','status','fill_utc_dt','exit_utc_dt',
                'r','wait_h','entry_type','setup_type']                                   # V6.125: tylko pola uzywane w petli
    x['_lim']=(x['entry_type'].astype(str)!='MARKET').astype(int)             # V6.87
    out=[]; weekly_rows=[]; exit_weekly_rows=[]; all_closed_events=[]
    for variant in warianty:
        # 'active' zawiera zarezerwowane sloty: pozycje i oczekujace LIMIT.
        active=[]; selected=[]; maxside={'LONG':0,'SHORT':0}; used=set()
        unfilled=cancelled=missing_fill=w_toku=0; orders=[]
        # V6.97 (szybkosc): jedno stabilne sortowanie calosci [zdjecie + klucze rankingu] zamiast sortowania
        # kazdego zdjecia osobno i iterrows - ta sama kolejnosc (stabilny lexsort, NaN na koncu), wynik identyczny.
        pierwsza={'ELLIOTT_HIPOTEZA':['_ell'],'MARKET_PIERWSZE':['_lim']}.get(variant,[])
        sortcols=pierwsza+['score_R','rr_tp1_net','cost_R','symbol','event_id']
        asc=[True]*len(pierwsza)+[False,False,True,True,True]
        # Wrazliwosc na arbitralna kolejnosc przy remisie: zmienia sie
        # tylko porzadek symbol/event_id, a nie oceny ani wynik przyszly.
        if tie_reverse:
            asc[-2:] = [False, False]
        # Audit only: retain the actual available source fields rather than hashing missing placeholders.
        # This does not affect ordering, selected candidates, or order execution.
        audit_columns=([c for c in _AUDIT130_SOURCE_FIELDS if c in x.columns and c not in _POTRZEBNE]
                       if audit_trace else [])
        _rek=x.sort_values(['snapshot_utc_dt']+sortcols,ascending=[True]+asc,kind='stable',
                           na_position='last')[_POTRZEBNE+audit_columns].to_dict('records')
        _grupy=[]
        for _r in _rek:
            if not _grupy or _grupy[-1][0]!=_r['snapshot_utc_dt']:
                _grupy.append((_r['snapshot_utc_dt'],[]))
            _grupy[-1][1].append(_r)
        for snap,candidates in _grupy:
            # Aktualizuj zlecenia wg zdarzen, ktore nastapily DO chwili zdjecia.
            # Fill LIMIT moze zmienic oczekujace zlecenie w pozycje (slot bez przerwy).
            remaining=[]
            for a in active:
                if a['release']<=snap:
                    continue
                if a.get('stan')=='LIMIT_OCZEKUJE' and pd.notna(a.get('fill')) and a['fill']<=snap:
                    a['stan']='POZYCJA'
                remaining.append(a)
            active=remaining
            for r in candidates:
                side=r['side']; sym=r['symbol']; evid=str(r['event_id'])
                if evid in used: continue
                if sum(a['side']==side for a in active)>=miejsca: continue
                if any(a['symbol']==sym for a in active): continue
                # Od tego miejsca decyzja jest podjeta. Dopiero teraz mozna
                # wykorzystac wynik symulacji do wyznaczenia zajetosci slotu.
                used.add(evid)
                status=str(r['status'])
                order={'wariant':variant,'snapshot':snap,'symbol':sym,'side':side, 'setup_type':r.get('setup_type',''), 'event_id':evid, 'entry_type':r['entry_type'], 'status':status,'r':None,'exit':pd.NaT}
                if audit_trace:
                    audit_src={f:_audit130_norm(r.get(f)) for f in _AUDIT130_SOURCE_FIELDS}
                    order['audit130_source']=audit_src
                    order['audit130_source_sha256']=_audit130_sha(audit_src)
                    order['audit130_engine_sha256']=_audit130_engine()
                fill=r['fill_utc_dt']; end=r['exit_utc_dt']
                is_final=status in FINAL and pd.notna(r['r']) and np.isfinite(r['r'])
                wait=r['wait_h']
                ttl_h=float(wait) if pd.notna(wait) and 0<float(wait)<=72 else (24.0 if r['entry_type']=='LIMIT' else 1/12)
                deadline=snap+pd.Timedelta(hours=ttl_h)
                filled=(is_final and pd.notna(fill) and pd.notna(end) and fill>=snap and end>=fill
                        and (r['entry_type']!='LIMIT' or fill<=deadline))
                if filled:
                    selected.append(r)
                    order['r']=float(r['r'])-float(extra_cost_R)
                    order['exit']=end
                    release=max(snap+pd.Timedelta(seconds=1),end)
                elif status in ('otwarty','oczekuje'):
                    w_toku+=1
                    if status=='oczekuje' and r['entry_type']=='LIMIT':
                        wait=r['wait_h']
                        ttl=float(wait) if pd.notna(wait) and 0<float(wait)<=72 else 24.0
                        release=snap+pd.Timedelta(hours=ttl)
                    else:
                        # Otwarta pozycja bez znanego zamkniecia trzyma slot do konca danych.
                        release=pd.Timestamp.max.tz_localize('UTC')
                else:
                    if is_final: missing_fill+=1
                    if status=='anulowany': cancelled+=1
                    else: unfilled+=1
                    # Realna chwila anulowania (z symulacji) lub TTL z parametru
                    # znanego podczas tworzenia sygnalu; brak danych: 24h LIMIT,
                    # 5 min MARKET. Czas wplywa tylko na pozniejsze snapy.
                    wait=r['wait_h']
                    ttl=float(wait) if pd.notna(wait) and 0<float(wait)<=72 else (24.0 if r['entry_type']=='LIMIT' else 1/12)
                    deadline=snap+pd.Timedelta(hours=ttl)
                    # Nie wykorzystuj przyszlego exit_utc rekordu niewypelnionego do
                    # przyspieszania wyboru nastepnych sygnalow; tylko znane TTL.
                    release=deadline
                    release=max(snap+pd.Timedelta(seconds=1), release)
                orders.append(order)
                active.append({'symbol':sym,'side':side,'release':release,
                               'stan':('LIMIT_OCZEKUJE' if r['entry_type']=='LIMIT' and filled and fill>snap
                                       else 'POZYCJA' if filled or status=='otwarty' else 'LIMIT_OCZEKUJE'),
                               'fill':fill if filled else pd.NaT})
                maxside[side]=max(maxside[side],sum(a['side']==side for a in active))
        if (return_weekly or return_exit_weekly) and orders:
            wk=pd.DataFrame(orders)
            wk['tydzien_utc']=wk['snapshot'].dt.tz_localize(None).dt.to_period('W-SUN').dt.start_time.dt.strftime('%Y-%m-%d')
            for week, g in wk.groupby('tydzien_utc',sort=True):
                rs=pd.to_numeric(g['r'],errors='coerce').dropna()
                weekly_rows.append({'wariant':variant,'tydzien_utc':week,
                                    'zlecenia_wybrane':int(len(g)),'transakcje':int(len(rs)),
                                    'suma_R':round(float(rs.sum()),3),
                                    'sredni_R_netto':round(float(rs.mean()),4) if len(rs) else None,
                                    'win_rate_pct':round(float((rs>0).mean()*100),2) if len(rs) else None,
                                    'tydzien_dodatni':bool(rs.sum()>0) if len(rs) else None,
                                    'uwaga':'okres wedlug daty sygnalu; tylko zamkniete pozycje w R'})
            if return_exit_weekly:
                closed=wk[wk['exit'].notna() & wk['r'].notna()].copy()
                if len(closed):
                    closed['tydzien_zamkniecia_utc']=closed['exit'].dt.tz_localize(None).dt.to_period('W-SUN').dt.start_time.dt.strftime('%Y-%m-%d')
                    for week, group in closed.groupby('tydzien_zamkniecia_utc',sort=True):
                        rvals=pd.to_numeric(group['r'],errors='coerce').dropna()
                        exit_weekly_rows.append({'wariant':variant,'tydzien_zamkniecia_utc':week,
                            'transakcje':len(rvals),'suma_R':round(float(rvals.sum()),3),
                            'sredni_R_netto':round(float(rvals.mean()),4) if len(rvals) else None,
                            'win_rate_pct':round(float((rvals>0).mean()*100),2) if len(rvals) else None})
        all_closed_events.extend({**o} for o in orders if pd.notna(o['exit']) and o['r'] is not None)
        vals=[float(r['r'])-float(extra_cost_R) for r in selected]
        # Drawdown obliczany wedlug faktycznych dat zamkniecia, nie dat sygnalow.
        # Zero startowe stanowi pierwszy szczyt equity w jednostkach R.
        exits=sorted(((o['exit'],float(o['r'])) for o in orders
                      if pd.notna(o['exit']) and o['r'] is not None),key=lambda z:z[0])
        equity=peak=max_dd=0.0
        for _, delta in exits:
            equity+=delta
            peak=max(peak,equity)
            max_dd=max(max_dd,peak-equity)
        out.append({'wariant':variant,'zlecenia_wybrane':len(used),
                    'transakcje':len(vals),'niewypelnione':unfilled,'anulowane':cancelled,'w_toku':w_toku,
                    'zdjecia':len(set(r['snapshot_utc'] for r in selected)),
                    'suma_R':round(sum(vals),3),'sredni_R_netto':round(float(np.mean(vals)),4) if vals else None,
                    'win_rate_pct':round(100*sum(v>0 for v in vals)/len(vals),2) if vals else None,
                    'maks_aktywne_LONG':maxside['LONG'],'maks_aktywne_SHORT':maxside['SHORT'],
                    'brak_fill_utc':missing_fill,'max_drawdown_R':round(max_dd,4),
                    'uwaga':'BADAWCZE A/B: wybor bez znajomosci przyszlych fill; zarezerwowane LIMIT; brak pelnego live i depozytu'})
    elliott_portfel_ab._last_trade_events = all_closed_events
    result=pd.DataFrame(out,columns=cols)
    week_df=pd.DataFrame(weekly_rows,columns=['wariant','tydzien_utc','zlecenia_wybrane','transakcje','suma_R','sredni_R_netto','win_rate_pct','tydzien_dodatni','uwaga'])
    exit_df=pd.DataFrame(exit_weekly_rows,columns=['wariant','tydzien_zamkniecia_utc','transakcje','suma_R','sredni_R_netto','win_rate_pct'])
    if return_exit_weekly: return result, week_df, exit_df
    return (result,week_df) if return_weekly else result

MARKET_AB_POLOWA = '2026-10-20T00:00:00Z'      # V6.87: stala granica polow (nie dobierana do wyniku)
MARKET_AB_OOS_OD = '2026-10-27T00:00:00Z'


def market_ab_raport(df, miejsca=3):
    """V6.87: czy pierwszenstwo MARKET przed LIMIT poprawia portfel (niewypelnione LIMIT nie blokuja slotow).
    Tylko diagnostyka - nie zmienia rankingu, do_handlu ani PROBA."""
    cols=['populacja','okres','kolejnosc_remisow','wariant','zlecenia_wybrane','transakcje','niewypelnione',
          'suma_R','sredni_R_netto','win_rate_pct','max_drawdown_R','przewaga_vs_BAZA_R','uwaga']
    if df is None or not len(df) or 'snapshot_utc' not in df.columns:
        return pd.DataFrame(columns=cols)
    x=df.copy()
    x['_t']=pd.to_datetime(x['snapshot_utc'],utc=True,errors='coerce')
    if {'setup_type','side','entry_type'}.issubset(x.columns):
        wyl=[bool(typ_wylaczony(str(t),s,e)) for t,s,e in zip(x['setup_type'],x['side'],x['entry_type'])]
    else:
        wyl=[False]*len(x)
    pops={'FILTRY_Z_CHWILI_SYGNALU':x,'AKTUALNE_REGULY':x[~np.array(wyl,dtype=bool)]}
    polowa=pd.Timestamp(MARKET_AB_POLOWA); oos=pd.Timestamp(MARKET_AB_OOS_OD)
    rows=[]
    for pn,p in pops.items():
        okresy={'CALOSC':p,'1_POLOWA (do 19.10)':p[p['_t']<polowa],'2_POLOWA (od 20.10)':p[p['_t']>=polowa],
                'OOS (od 27.10)':p[p['_t']>=oos]}
        for on,q in okresy.items():
            for rev in (False,True):
                if rev and on!='CALOSC': continue
                ab=elliott_portfel_ab(q.drop(columns=['_t']),miejsca=miejsca,tie_reverse=rev,
                                      warianty=('BAZA','MARKET_PIERWSZE'))
                if not len(ab): continue
                base=ab.loc[ab['wariant']=='BAZA','suma_R']
                b=float(base.iloc[0]) if len(base) else 0.0
                for rec in ab.to_dict('records'):
                    rows.append({'populacja':pn,'okres':on,'kolejnosc_remisow':'odwrotna' if rev else 'normalna',
                                 **{k:rec.get(k) for k in cols if k in rec},
                                 'przewaga_vs_BAZA_R':round(float(rec['suma_R'])-b,3),
                                 'uwaga':'BADAWCZE: tylko kolejnosc wyboru; decyzja dopiero po OOS (>= 2 tygodnie)'})
    return pd.DataFrame(rows,columns=cols)



def raport_trzech_wejsc_oos(df, start='2026-10-27T00:00:00Z'):
    """Badawczy wynik rozliczonych okazji OOS; nie jest wynikiem portfela.

    Zlicza wyniki poszczegolnych zlecen (MARKET, LIMIT, swieze ABC) osobno.
    To nie jest alternatywny wariant selekcji zlecen ani pelny walk-forward.
    Koszty podstawowe sa zawarte w 'r'; dodatkowe koszty to stres-test.
    """
    cols=['kategoria','okres','dodatkowy_koszt_R','rozliczone_wiersze','niezalezne_okazje',
          'zdjecia_rynku','dni_sygnalow','suma_R_netto','sredni_R_netto',
          'win_rate_pct','dni_z_rozliczeniami','wynik_pierwsza_polowa_R',
          'wynik_druga_polowa_R','status_proby','uwaga']
    detail_cols=['kategoria','symbol','snapshot_utc','okazja_id','event_id',
                 'entry_type','elliott_abc_status','status','r_netto','uwaga']
    if df is None or df.empty or not {'snapshot_utc','entry_type','status','r'}.issubset(df.columns):
        return pd.DataFrame(columns=cols),pd.DataFrame(columns=detail_cols)
    x=elliott_tylko_po_filtrach(df).copy()
    x['_dt']=pd.to_datetime(x['snapshot_utc'],utc=True,errors='coerce')
    x=x[x['_dt'].notna() & (x['_dt']>=pd.Timestamp(start))].copy()
    if x.empty: return pd.DataFrame(columns=cols),pd.DataFrame(columns=detail_cols)
    x['_r']=pd.to_numeric(x['r'],errors='coerce')
    x=x[x['status'].isin(FINAL) & np.isfinite(x['_r'])].copy()
    if x.empty: return pd.DataFrame(columns=cols),pd.DataFrame(columns=detail_cols)
    if 'okazja_id' not in x.columns:
        x['okazja_id']=x.get('event_id',pd.Series(x.index.astype(str),index=x.index))
    x['okazja_id']=x['okazja_id'].fillna('').astype(str)
    empty=x['okazja_id'].eq('')
    x.loc[empty,'okazja_id']=x.index[empty].astype(str)
    x['_exit']=pd.to_datetime(x['exit_utc'],utc=True,errors='coerce') if 'exit_utc' in x else pd.NaT
    x['_day']=x['_dt'].dt.date.astype(str)
    x['_exit_day']=x['_exit'].dt.date.astype('string')
    x['_entry']=x['entry_type'].fillna('').astype(str).str.upper()
    abc=x.get('elliott_abc_status',pd.Series('',index=x.index)).fillna('').astype(str).str.upper()
    dist=pd.to_numeric(x.get('elliott_abc_odleglosc_atr',pd.Series(float('nan'),index=x.index)),errors='coerce')
    abc_side=x.get('elliott_abc_kierunek',pd.Series('',index=x.index)).fillna('').astype(str).str.upper()
    side=x.get('side',pd.Series('',index=x.index)).fillna('').astype(str).str.upper()
    # Sposob opisu korekty z wersji V6.103+: tylko jawnie zakonczone ABC
    # i dostepna odleglosc; brak danych nie znaczy "brak ABC".
    x['_abc_fresh']=abc.eq('ABC_POTENCJALNIE_ZAKONCZONE') & dist.ge(0) & dist.le(2.5) & abc_side.eq(side)
    detail=[]; rows=[]
    cut=x['_dt'].min()+(x['_dt'].max()-x['_dt'].min())/2
    group_masks={'MARKET':x['_entry'].eq('MARKET'),
                 'LIMIT':x['_entry'].eq('LIMIT'),
                 'ELLIOTT_ABC_SWIEZE':x['_abc_fresh']}
    for name,mask in group_masks.items():
        g=x.loc[mask].copy()
        for r in g.itertuples(index=False):
            detail.append({'kategoria':name,'symbol':getattr(r,'symbol',''),
                'snapshot_utc':getattr(r,'snapshot_utc',''),
                'okazja_id':getattr(r,'okazja_id',''),
                'event_id':getattr(r,'event_id',''),
                'entry_type':getattr(r,'entry_type',''),
                'elliott_abc_status':getattr(r,'elliott_abc_status',''),
                'status':getattr(r,'status',''),
                'r_netto':round(float(getattr(r,'_r',0)),5) if False else None,
                'uwaga':'Rozliczona obserwacja; kategorie ABC i MARKET/LIMIT moga sie nakladac'})
        for extra in (0.0,0.05,0.10):
            rr=g['_r']-extra
            n=len(g)
            day_count=int(g.loc[g['_exit'].notna(),'_exit_day'].nunique())
            first=g['_dt']<=cut
            rows.append({'kategoria':name,'okres':'OOS_OD_'+start[:10],
                'dodatkowy_koszt_R':extra,'rozliczone_wiersze':n,
                'niezalezne_okazje':int(g['okazja_id'].nunique()),
                'zdjecia_rynku':int(g['snapshot_utc'].nunique()),
                'dni_sygnalow':int(g['_day'].nunique()),
                'suma_R_netto':round(float(rr.sum()),4) if n else None,
                'sredni_R_netto':round(float(rr.mean()),4) if n else None,
                'win_rate_pct':round(float((rr>0).mean()*100),2) if n else None,
                'dni_z_rozliczeniami':day_count,
                'wynik_pierwsza_polowa_R':round(float(rr[first].sum()),4) if n else None,
                'wynik_druga_polowa_R':round(float(rr[~first].sum()),4) if n else None,
                'status_proby':('BRAK_ROZLICZEN' if not n else 'MALA_PROBA' if g['okazja_id'].nunique()<30 or day_count<10 else 'DO_DALSZEJ_WALIDACJI'),
                'uwaga':'ABC jest podzbiorem MARKET/LIMIT; wyniki nie sumuja sie. Filtry z archiwum, bez odtworzenia portfolio; koszt dodatkowy w R.'})
    de=pd.DataFrame(detail,columns=detail_cols)
    if len(de):
        # Przypisz r po iteracji w tym samym porzadku (itertuples modyfikuje nazwy _r).
        vals=[]
        for mask in group_masks.values(): vals.extend(x.loc[mask,'_r'].round(5).tolist())
        de['r_netto']=vals
    return pd.DataFrame(rows,columns=cols),de


def raport_parowany_wejsc_oos(df, start='2026-10-27T00:00:00Z'):
    """V6.123: porownanie MARKET/LIMIT tylko gdy obie alternatywy istnieja w tym samym snapshotcie.

    Wyniki rozliczonych zlecen; wspolny klucz (symbol, snapshot, setup_type, side).
    Oddziela brak wypelnienia od straty; nie dobiera wierszy na podstawie przyszlego wyniku.
    Raport diagnostyczny, a nie portfel, bez zmiany bramek i sygnalow.
    """
    paired_cols=['symbol','snapshot_utc','setup_type','side','market_event_id','limit_event_id',
                 'market_status','limit_status','market_r','limit_r','roznica_market_minus_limit_R',
                 'wynik_pary','uwaga']
    summary_cols=['okres','strategia','wariant','koszt_dodatkowy_R','pary_z_oboma_rozliczeniami',
                  'pary_dostepne','niezalezne_zdjecia','dni_sygnalow','srednia_roznica_R',
                  'suma_roznic_R','market_sredni_R','limit_sredni_R','market_lepszy_pct',
                  'bez_najlepszego_dnia_R','bez_najgorszego_dnia_R','pierwsza_polowa_R',
                  'druga_polowa_R','status_proby','uwaga']
    days_cols=['dzien_utc','strategia','pary','suma_roznic_R','srednia_roznica_R']
    empty=(pd.DataFrame(columns=summary_cols),pd.DataFrame(columns=paired_cols),pd.DataFrame(columns=days_cols))
    required={'symbol','snapshot_utc','setup_type','side','entry_type','status','r'}
    if df is None or df.empty or not required.issubset(df.columns): return empty
    x=elliott_tylko_po_filtrach(df).copy()
    x['_date']=pd.to_datetime(x['snapshot_utc'],utc=True,errors='coerce')
    x=x[x['_date'].notna() & (x['_date']>=pd.Timestamp(start))].copy()
    x['_entry']=x['entry_type'].fillna('').astype(str).str.upper()
    x=x[x['_entry'].isin(('MARKET','LIMIT'))].copy()
    x['_r']=pd.to_numeric(x['r'],errors='coerce')
    x['_resolved']=x['status'].isin(FINAL) & np.isfinite(x['_r'])
    if x.empty: return empty
    keys=['symbol','snapshot_utc','setup_type','side']
    # Tylko faktyczne pary z obu rodzajow wejsc; duplikaty klucza nie sa arbitralnie wybierane.
    x['_duplicate']=x.duplicated(keys+['_entry'],keep=False)
    x=x[~x['_duplicate']]
    found=x.groupby(keys,dropna=False)['_entry'].nunique()
    pair_keys=found[found.eq(2)].index
    if not len(pair_keys):return empty
    ix=pd.MultiIndex.from_frame(x[keys]);x=x[ix.isin(pair_keys)].copy()
    market=x[x['_entry']=='MARKET'].set_index(keys)
    limit=x[x['_entry']=='LIMIT'].set_index(keys)
    common=market.index.intersection(limit.index)
    rows=[]
    for key in common:
        m=market.loc[key];l=limit.loc[key]
        both=bool(m['_resolved'] and l['_resolved'])
        mr=float(m['_r']) if m['_resolved'] else None
        lr=float(l['_r']) if l['_resolved'] else None
        rows.append(dict(zip(keys,key)) | {'market_event_id':m.get('event_id',''),
            'limit_event_id':l.get('event_id',''),'market_status':m['status'],
            'limit_status':l['status'],'market_r':mr,'limit_r':lr,
            'roznica_market_minus_limit_R':round(mr-lr,6) if both else None,
            'wynik_pary':'OBIE_ROZLICZONE' if both else 'NIEPELNA_PARA',
            'uwaga':'Wyniki obserwacyjne; LIMIT moze pozostac niewypelniony'})
    detail=pd.DataFrame(rows,columns=paired_cols)
    resolved=detail[detail['wynik_pary'].eq('OBIE_ROZLICZONE')].copy()
    if len(resolved):
        resolved['_date']=pd.to_datetime(resolved['snapshot_utc'],utc=True)
        resolved['_day']=resolved['_date'].dt.strftime('%Y-%m-%d')
    daily=[]; summaries=[]
    for strategy in ['WSZYSTKIE']+sorted(detail['setup_type'].dropna().astype(str).unique().tolist()):
        d=resolved if strategy=='WSZYSTKIE' else resolved[resolved['setup_type'].astype(str).eq(strategy)]
        available=len(detail) if strategy=='WSZYSTKIE' else int(detail['setup_type'].astype(str).eq(strategy).sum())
        if len(d):
            ds=d.groupby('_day',sort=True)['roznica_market_minus_limit_R'].agg(['sum','mean','count'])
            for day,row in ds.iterrows(): daily.append({'dzien_utc':day,'strategia':strategy,'pary':int(row['count']),
                'suma_roznic_R':round(float(row['sum']),5),'srednia_roznica_R':round(float(row['mean']),5)})
        else:ds=pd.DataFrame(columns=['sum','mean','count'])
        for cost in (0.,0.05,0.10):
            # Symetryczny koszt per wykonana transakcje znosi sie dla par.
            # Stres asymetryczny: +cost dla MARKET (bardziej agresywne wykonanie).
            delta=d['roznica_market_minus_limit_R']-cost if len(d) else pd.Series(dtype=float)
            daily_sums=d.groupby('_day')['roznica_market_minus_limit_R'].sum() if len(d) else pd.Series(dtype=float)
            date_cut=d['_date'].min()+(d['_date'].max()-d['_date'].min())/2 if len(d) else None
            summaries.append({'okres':'OOS_OD_'+start[:10],'strategia':strategy,'wariant':'DODATKOWY_KOSZT_MARKET',
                'koszt_dodatkowy_R':cost,'pary_z_oboma_rozliczeniami':len(d),
                'pary_dostepne':available,'niezalezne_zdjecia':int(d['snapshot_utc'].nunique()),
                'dni_sygnalow':int(d['_day'].nunique()),
                'srednia_roznica_R':round(float(delta.mean()),5) if len(d) else None,
                'suma_roznic_R':round(float(delta.sum()),5) if len(d) else None,
                'market_sredni_R':round(float((d['market_r']-cost).mean()),5) if len(d) else None,
                'limit_sredni_R':round(float(d['limit_r'].mean()),5) if len(d) else None,
                'market_lepszy_pct':round(float((delta>0).mean()*100),2) if len(d) else None,
                'bez_najlepszego_dnia_R':round(float(delta.sum()-(daily_sums-cost*d.groupby('_day').size()).max()),5) if len(daily_sums) else None,
                'bez_najgorszego_dnia_R':round(float(delta.sum()-(daily_sums-cost*d.groupby('_day').size()).min()),5) if len(daily_sums) else None,
                'pierwsza_polowa_R':round(float(delta[d['_date']<=date_cut].sum()),5) if len(d) else None,
                'druga_polowa_R':round(float(delta[d['_date']>date_cut].sum()),5) if len(d) else None,
                'status_proby':'BRAK_PAR' if not len(d) else 'MALA_PROBA' if len(d)<30 or d['_day'].nunique()<10 else 'DO_WALIDACJI',
                'uwaga':'Oba zlecenia maja ten sam snapshot/symbol/typ/strone; tylko obie rozliczone. Pozostale pary w szczegolach. Wynik bez modelu portfela.'})
    return pd.DataFrame(summaries,columns=summary_cols),detail,pd.DataFrame(daily,columns=days_cols)


def raport_decyzji_wejsc_oos(df, start='2026-10-27T00:00:00Z', min_par=30, min_dni=10):
    """Badawcze porownanie wspolnych decyzji, bez selekcji po przyszlym fill.

    Brak wykonania LIMIT liczy sie jako 0R TYLKO po potwierdzonym wygasnieciu
    lub anulowaniu. Otwarty/niepewny rezultat jest cenzurowany i nie jest
    zastępowany zerem. Chronologiczny portfel uzywa istniejacego silnika
    rezerwujacego sloty do fill/TTL/exit.
    """
    detail_cols=['symbol','snapshot_utc','setup_type','side','market_event_id','limit_event_id',
        'market_status','limit_status','market_decyzja_R','limit_decyzja_R','roznica_R',
        'market_wykonany','limit_wykonany','para_ocenialna','status_porownania']
    summary_cols=['wariant','koszt_dodatkowy_market_R','pary_wszystkie','pary_ocenialne',
        'pary_cenzurowane','market_wykonane','limit_wykonane','limit_bez_wykonania',
        'dni_niezalezne','srednia_roznica_R','suma_roznic_R','wynik_bez_najlepszego_dnia_R',
        'wynik_bez_najgorszego_dnia_R','status','uwaga']
    port_cols=['wariant','miejsca_kierunek','koszt_dodatkowy_R','wybrane',
        'rozliczone','niewypelnione','anulowane','w_toku','suma_R',
        'status','uwaga']
    empty=(pd.DataFrame(columns=summary_cols),pd.DataFrame(columns=detail_cols),
           pd.DataFrame(columns=port_cols))
    required={'symbol','snapshot_utc','setup_type','side','entry_type','status','r'}
    if df is None or df.empty or not required.issubset(df.columns):return empty
    x=elliott_tylko_po_filtrach(df).copy()
    x['_time']=pd.to_datetime(x['snapshot_utc'],utc=True,errors='coerce')
    x=x[x['_time'].notna() & (x['_time']>=pd.Timestamp(start))].copy()
    x['_entry']=x['entry_type'].fillna('').astype(str).str.upper()
    x=x[x['_entry'].isin(('MARKET','LIMIT'))].copy()
    if x.empty:return empty
    keys=['symbol','snapshot_utc','setup_type','side']
    # Fail-closed: nie wybieramy arbitralnie jednej z dwoch zduplikowanych ofert.
    x=x.loc[~x.duplicated(keys+['_entry'],keep=False)].copy()
    found=x.groupby(keys,dropna=False)['_entry'].nunique()
    both=found[found.eq(2)].index
    if not len(both):return empty
    x=x.loc[pd.MultiIndex.from_frame(x[keys]).isin(both)].copy()
    market=x[x['_entry'].eq('MARKET')].set_index(keys)
    limit=x[x['_entry'].eq('LIMIT')].set_index(keys)
    rows=[]
    # Uznajemy brak fill za 0R tylko dla jednoznacznych koncowych statusow.
    no_fill={'niewypelniony','anulowany','wygasly','expired','cancelled'}
    for key in market.index.intersection(limit.index):
        m=market.loc[key];l=limit.loc[key]
        def result(z):
            status=str(z.get('status','')).strip().lower()
            r=pd.to_numeric(z.get('r'),errors='coerce')
            if z.get('status') in FINAL and pd.notna(r) and np.isfinite(r):
                return float(r),True,'ROZLICZONE'
            if status in no_fill and pd.isna(pd.to_datetime(z.get('fill_utc'),utc=True,errors='coerce')):
                return 0.0,False,'BRAK_WYKONANIA_FINALNY'
            return None,False,'CENZUROWANE'
        mr,mfill,ms=result(m);lr,lfill,ls=result(l)
        good=mr is not None and lr is not None
        rows.append(dict(zip(keys,key))|{'market_event_id':m.get('event_id',''),
            'limit_event_id':l.get('event_id',''), 'market_status':m.get('status'),
            'limit_status':l.get('status'),'market_decyzja_R':mr,'limit_decyzja_R':lr,
            'roznica_R':round(mr-lr,6) if good else None,'market_wykonany':mfill,
            'limit_wykonany':lfill,'para_ocenialna':good,
            'status_porownania':('OBIE_ROZLICZONE' if mfill and lfill else 'DECYZJE_OCENIALNE')
                if good else 'CENZUROWANE_'+ms+'_'+ls})
    detail=pd.DataFrame(rows,columns=detail_cols)
    valid=detail.loc[detail['para_ocenialna']].copy()
    if len(valid):valid['_day']=pd.to_datetime(valid['snapshot_utc'],utc=True).dt.strftime('%Y-%m-%d')
    summary=[]
    for cost in (0.,0.05,0.10):
        delta=valid['roznica_R']-cost*valid['market_wykonany'].astype(float) if len(valid) else pd.Series(dtype=float)
        daily=(pd.DataFrame({'day':valid['_day'],'delta':delta}).groupby('day')['delta'].sum()
               if len(valid) else pd.Series(dtype=float))
        n_days=int(valid['_day'].nunique()) if len(valid) else 0
        total=float(delta.sum()) if len(valid) else None
        positive=(len(valid)>=min_par and n_days>=min_dni and total is not None and total>0 and
                  all(float((valid['roznica_R']-v*valid['market_wykonany'].astype(float)).sum())>0 for v in (0.,0.05,0.10)) and
                  len(daily)>1 and total-float(daily.max())>0)
        summary.append({'wariant':'WSPOLNE_DECYZJE_MARKET_MINUS_LIMIT',
            'koszt_dodatkowy_market_R':cost,'pary_wszystkie':len(detail),
            'pary_ocenialne':len(valid),'pary_cenzurowane':len(detail)-len(valid),
            'market_wykonane':int(valid['market_wykonany'].sum()) if len(valid) else 0,
            'limit_wykonane':int(valid['limit_wykonany'].sum()) if len(valid) else 0,
            'limit_bez_wykonania':int((~valid['limit_wykonany'].astype(bool)).sum()) if len(valid) else 0,
            'dni_niezalezne':n_days,'srednia_roznica_R':round(float(delta.mean()),5) if len(valid) else None,
            'suma_roznic_R':round(total,5) if total is not None else None,
            'wynik_bez_najlepszego_dnia_R':round(total-float(daily.max()),5) if len(daily) else None,
            'wynik_bez_najgorszego_dnia_R':round(total-float(daily.min()),5) if len(daily) else None,
            'status':'WYMAGA_NIEZALEZNEJ_WALIDACJI' if positive else 'BRAK_POTWIERDZONEJ_PRZEWAGI',
            'uwaga':'Decyzje sparowane as-of; finalny brak fill 0R; otwarte/niepewne cenzurowane; koszty tylko gdy MARKET wykonany. Nie odblokowuje handlu.'})
    portfolio=[]
    # Porownanie odrebnych wyborow na tej samej dostepnej populacji par;
    # istniejacy silnik uwzglednia TTL, zajetosc symbolu i limit pozycji na kierunek.
    cutoff=x['_time'].max()
    for slots in (2,3):
        for typ in ('MARKET','LIMIT'):
            sub=x.loc[x['_entry'].eq(typ)].drop(columns=['_entry','_time'])
            for cost in (0.,0.05,0.10):
                tab=elliott_portfel_ab(sub,miejsca=slots,extra_cost_R=cost if typ=='MARKET' else 0.0,
                                      warianty=('BAZA',),asof_utc=cutoff)
                if tab.empty:continue
                r=tab.iloc[0]
                portfolio.append({'wariant':typ,'miejsca_kierunek':slots,
                    'koszt_dodatkowy_R':cost,'wybrane':int(r['zlecenia_wybrane']),
                    'rozliczone':int(r['transakcje']),'niewypelnione':int(r['niewypelnione']),
                    'anulowane':int(r['anulowane']),'w_toku':int(r['w_toku']),
                    'suma_R':float(r['suma_R']),'status':'BRAK_POTWIERDZONEJ_PRZEWAGI',
                    'uwaga':'Badawczy portfel tylko wspolnych kandydatur; wybór chronologiczny, TTL i sloty; nie symulacja rzeczywistego depozytu.'})
    return pd.DataFrame(summary,columns=summary_cols),detail,pd.DataFrame(portfolio,columns=port_cols)


# V6.130 — stabilne odciski dla diagnostyki, bez wplywu na strategie.
def _audit130_norm(value):
    if value is None:return None
    try:
        if pd.isna(value):return None
    except (TypeError,ValueError):pass
    if isinstance(value,(pd.Timestamp,datetime)):
        t=pd.to_datetime(value,utc=True,errors='coerce')
        return t.isoformat() if pd.notna(t) else None
    if isinstance(value,(np.integer,)):return int(value)
    if isinstance(value,(float,np.floating)):
        return float(value) if np.isfinite(value) else None
    if isinstance(value,(bool,int,str)):return value
    return str(value)

_AUDIT130_SOURCE_FIELDS=('entry','sl','tp1','tp2','cost_R','funding_R','slip_bps',
    'risk_pct','rr_tp1_net','metoda_sym','wait_h','fill_utc','exit_utc','status',
    'mfe_R','mae_R','bars_held','niejednoznaczne','luka_danych')

def _audit130_sha(record):
    payload={k:_audit130_norm(v) for k,v in sorted(record.items())}
    return hashlib.sha256(json.dumps(payload,sort_keys=True,ensure_ascii=False,separators=(',',':')).encode('utf-8')).hexdigest()

def _audit130_engine():
    import marshal
    # Odcisk kodu funkcji, nie etykiety wersji; zmiana silnika jest wykrywalna.
    return hashlib.sha256(marshal.dumps(elliott_portfel_ab.__code__)).hexdigest()

def audit130_diagnose(now, prior, tolerance_R=0.001):
    cols=['wariant','event_id','symbol','snapshot_utc','typ_zmiany','delta_R',
          'delta_R_w_tolerancji','zmiana_parametrow_wejscia','zmiana_kosztow',
          'zmiana_danych_symulacji','zmiana_kodu_silnika','kompletny_snapshot_OHLCV',
          'interpretacja']
    keys=['wariant','event_id','symbol','snapshot_utc']
    def idx(df):
        if df is None or df.empty:return {}
        if not set(keys).issubset(df.columns):raise ValueError('V6.130: brak kluczy checkpointu')
        result={}
        for row in rekordy_audytu_v174(df, 'df_audit'):
            key=tuple(str(row.get(k,'')) for k in keys)
            if key in result:raise ValueError('V6.130: duplikat klucza')
            result[key]=row
        return result
    a,b=idx(prior),idx(now)
    result=[]
    def changed(x,y,fields):
        if x is None or y is None:return None
        if not all(f in x and f in y for f in fields):return None
        return any(_audit130_norm(x[f])!=_audit130_norm(y[f]) for f in fields)
    for k in sorted(set(a)|set(b)):
        old,cur=a.get(k),b.get(k)
        if old is None or cur is None:
            label='BRAK_POROWNANIA' if prior is None else 'NOWY' if old is None else 'NIEOBECNY'
            delta=None
        else:
            ro=pd.to_numeric(old.get('r_netto'),errors='coerce');rn=pd.to_numeric(cur.get('r_netto'),errors='coerce')
            delta=float(rn-ro) if pd.notna(ro) and pd.notna(rn) else None
            label='ZMIANA_R' if delta is not None and abs(delta)>1e-8 else 'BEZ_ZMIAN_R'
        setup=changed(old,cur,['entry','sl','tp1','tp2','risk_pct'])
        costs=changed(old,cur,['cost_R','funding_R','slip_bps'])
        source=changed(old,cur,['source_sha256'])
        engine=changed(old,cur,['engine_sha256'])
        complete=bool(old and cur and old.get('ohlcv_snapshot_sha256') and cur.get('ohlcv_snapshot_sha256'))
        if label!='ZMIANA_R': interpretation=label
        elif setup is True:interpretation='ZMIENIONE_POZIOMY_LUB_RYZYKO'
        elif costs is True:interpretation='ZMIENIONE_PARAMETRY_KOSZTOW'
        elif engine is True:interpretation='ZMIENIONY_KOD_SILNIKA'
        elif source is True:interpretation='ZMIENIONE_DANE_LUB_WYNIK_SYMULACJI'
        elif any(v is None for v in (setup,costs,source,engine)):
            interpretation='NIEPELNE_DANE_POPRZEDNIEGO_CHECKPOINTU'
        else:interpretation='NIEUSTALONA_PRZYCZYNA_BEZ_OHLCV'
        result.append(dict(zip(keys,k),typ_zmiany=label,delta_R=delta,
             delta_R_w_tolerancji=(abs(delta)<=tolerance_R if delta is not None else None),
             zmiana_parametrow_wejscia=setup,zmiana_kosztow=costs,
             zmiana_danych_symulacji=source,zmiana_kodu_silnika=engine,
             kompletny_snapshot_OHLCV=complete,interpretacja=interpretation))
    return pd.DataFrame(result,columns=cols)

# V6.131: diagnostyka metadanych oddzielona od faktycznej zmiany wartosci R.
def _audit132_presence(v):
    """Missing CSV fields are UNKNOWN, never an actual change."""
    if v is None:return False
    try:
        if bool(pd.isna(v)):return False
    except (TypeError,ValueError):pass
    return not (isinstance(v,str) and v.strip().lower() in ('','nan','none','null','nat'))

def _audit132_compare(a,b,fields):
    """Three-valued comparison: ZMIENIONE / BEZ_ZMIAN / NIEPOROWNYWALNE.

    A multi-field group is ZMIENIONE only when an actually present pair differs;
    incomplete remaining fields cannot conceal that difference.
    """
    if a is None or b is None:return 'NIEPOROWNYWALNE'
    missing=False
    for key in fields:
        if key not in a or key not in b or not _audit132_presence(a[key]) or not _audit132_presence(b[key]):
            missing=True;continue
        x=_audit130_norm(a[key]); y=_audit130_norm(b[key])
        if _audit135_equal(x,y):
            continue
        if x!=y:
            # CSV checkpoints store numbers as strings; their representation must
            # not itself create a spurious numerical change.
            try:
                xx=float(x); yy=float(y)
                if np.isfinite(xx) and np.isfinite(yy) and xx==yy:continue
            except (TypeError,ValueError):pass
            return 'ZMIENIONE'
    return 'NIEPOROWNYWALNE' if missing else 'BEZ_ZMIAN'

def audit131_classify(now, prior, tolerance_R=0.001):
    """V6.132: checkpoint-aware audit; missing historical fields are unknown.

    Preserve V6.131 output columns for downstream readers, adding explicit
    three-state comparison columns and reason of non-comparability.
    """
    columns=['wariant','event_id','symbol','snapshot_utc','status131','delta_R',
             'w_tolerancji_R','zmiana_czasu_wyjscia','zmiana_metadanych',
             'zmiana_poziomow','zmiana_kosztow','zmiana_kodu','zmiana_zrodla',
             'status_danych_OHLCV','porownanie_czasu_wyjscia','porownanie_metadanych',
             'porownanie_poziomow','porownanie_kosztow','porownanie_kodu',
             'porownanie_zrodla','porownanie_OHLCV','kompletnosc_checkpointu']
    keys=('wariant','event_id','symbol','snapshot_utc')
    def keyed(df):
        if df is None:return None
        if len(df)==0:return {}
        if not set(keys).issubset(df.columns):raise ValueError('V6.132: brak kluczy checkpointu')
        out={}
        for r in rekordy_audytu_v174(df, 'df_audit'):
            key=tuple(str(r[k]) for k in keys)
            if key in out:raise ValueError('V6.132: duplikat rekordu')
            out[key]=r
        return out
    old=keyed(prior);cur=keyed(now) or {}; rows=[]
    groups={
        'czasu_wyjscia':('exit_utc',),
        'metadanych':('setup_type','entry_type','side','status'),
        'poziomow':('entry','sl','tp1','tp2','risk_pct'),
        'kosztow':('cost_R','funding_R','slip_bps'),
        'kodu':('engine_sha256',),
        'zrodla':('source_sha256',),
        'OHLCV':('ohlcv_snapshot_sha256',),
    }
    for k in sorted(set(cur)|set(old or {})):
        a=(old or {}).get(k); b=cur.get(k)
        states={name:_audit132_compare(a,b,fields) for name,fields in groups.items()}
        if a is None or b is None:
            delta=None
            typ=('BRAK_POROWNANIA' if old is None else 'NOWY' if a is None else 'NIEOBECNY')
        else:
            ra=pd.to_numeric(a.get('r_netto'),errors='coerce');rb=pd.to_numeric(b.get('r_netto'),errors='coerce')
            delta=float(rb-ra) if pd.notna(ra) and pd.notna(rb) else None
            if delta is None:typ='NIEPOROWNYWALNY_R'
            elif abs(delta)>1e-8:typ='ZMIANA_R'
            elif any(x=='ZMIENIONE' for x in states.values()):typ='ZMIANA_TYLKO_METADANYCH'
            elif any(x=='NIEPOROWNYWALNE' for x in states.values()):typ='NIEPOROWNYWALNE_METADANE'
            else:typ='BEZ_ZMIAN'
        rows.append(dict(zip(keys,k),status131=typ,delta_R=delta,
            w_tolerancji_R=(abs(delta)<=tolerance_R if delta is not None else None),
            **{'zmiana_'+name:(True if states[name]=='ZMIENIONE' else False if states[name]=='BEZ_ZMIAN' else None)
               for name in ('czasu_wyjscia','metadanych','poziomow','kosztow','kodu','zrodla')},
            status_danych_OHLCV=('POROWNYWALNE_HASH' if states['OHLCV']!='NIEPOROWNYWALNE' else 'BRAK_DWOCH_SNAPSHOTOW'),
            **{'porownanie_'+name:state for name,state in states.items()},
            kompletnosc_checkpointu=('PELNY' if all(x!='NIEPOROWNYWALNE' for x in states.values()) else 'NIEPELNY')))
    return pd.DataFrame(rows,columns=columns)

# V6.134 — rozdziel dane wejściowe od wyników symulacji i pokaż wartości przed/po.
_AUDIT134_INPUT_FIELDS=('entry','sl','tp1','tp2','risk_pct','cost_R','funding_R',
                        'slip_bps','rr_tp1_net','metoda_sym','wait_h')
_AUDIT134_OUTPUT_FIELDS=('status','fill_utc','exit_utc','mfe_R','mae_R',
                         'bars_held','niejednoznaczne','luka_danych')

def _audit135_equal(left,right):
    """Strict bool normalization; only true/false textual spellings compare as booleans.

    CSV 'False' must match False, but numeric 0 should not silently match False
    for unrelated fields (e.g. hashes, symbols, metadata).
    """
    def parse(v):
        if isinstance(v,(bool,np.bool_)):return ('bool',bool(v))
        if isinstance(v,str):
            t=v.strip().lower()
            if t in ('true','false'):return ('bool',t=='true')
        if isinstance(v,(int,float,np.integer,np.floating)) and not isinstance(v,(bool,np.bool_)):
            try:
                n=float(v)
                if np.isfinite(n):return ('number',n)
            except (ValueError,TypeError):pass
        if isinstance(v,str):
            try:
                n=float(v.strip())
                if np.isfinite(n):return ('number',n)
            except (ValueError,TypeError):pass
        return ('literal',_audit130_norm(v))
    return parse(left)==parse(right)

def _audit134_equal(left,right):
    return _audit135_equal(left,right)

def audit134_details(now,prior,tolerance_R=0.001):
    """Only two present values establish change. Hash is not OHLCV evidence."""
    key=('wariant','event_id','symbol','snapshot_utc')
    def indexed(frame):
        if frame is None:return None
        if frame.empty:return {}
        if not set(key).issubset(frame.columns):raise ValueError('V6.134: missing checkpoint keys')
        records={}
        for r in rekordy_audytu_v174(frame, 'frame_audit'):
            k=tuple(str(r[f]) for f in key)
            if k in records:raise ValueError('V6.134: duplicate event key')
            records[k]=r
        return records
    a=indexed(prior);b=indexed(now) or {}
    fields=(*_AUDIT134_INPUT_FIELDS,*_AUDIT134_OUTPUT_FIELDS,'source_sha256',
            'engine_sha256','ohlcv_snapshot_sha256','r_netto')
    result=[]
    for k in sorted(set(a or {})|set(b)):
        old=(a or {}).get(k);cur=b.get(k)
        for field in fields:
            o=old.get(field) if old is not None else None
            n=cur.get(field) if cur is not None else None
            if old is None or cur is None or not _audit132_presence(o) or not _audit132_presence(n):
                status='NIEPOROWNYWALNE'
            else:status='BEZ_ZMIAN' if _audit134_equal(o,n) else 'ZMIENIONE'
            delta=None
            if field=='r_netto' and status!='NIEPOROWNYWALNE':
                try:delta=float(n)-float(o)
                except (TypeError,ValueError):pass
            if field in _AUDIT134_INPUT_FIELDS:group='PARAMETRY_I_KOSZTY'
            elif field in _AUDIT134_OUTPUT_FIELDS or field=='r_netto':group='WYNIK_SYMULACJI'
            elif field=='source_sha256':group='MIESZANY_HASH_WEJSCIA_I_WYNIKU'
            elif field=='engine_sha256':group='KOD_SILNIKA'
            else:group='SWIECE_OHLCV'
            reason=('HASH_MIESZANY_NIE_DOWODZI_ZMIANY_OHLCV' if field=='source_sha256'
                    else 'BRAK_POROWNYWALNYCH_SNAPSHOTOW' if field=='ohlcv_snapshot_sha256' and status=='NIEPOROWNYWALNE'
                    else '')
            result.append(dict(zip(key,k),grupa=group,pole=field,
                przed=None if not _audit132_presence(o) else str(_audit130_norm(o)),
                po=None if not _audit132_presence(n) else str(_audit130_norm(n)),
                porownanie=status,delta_R=delta,
                zmiana_R_do_001=(abs(delta)<=tolerance_R if delta is not None else None),
                interpretacja=reason))
    return pd.DataFrame(result)

def audit134_checkpoints(now,prior):
    """Current-only stable input/output hashes; older versions remain unknown."""
    fields=['wariant','event_id','symbol','snapshot_utc','input_sha256','output_sha256',
            'status_input','status_output','uwaga']
    if now is None or now.empty:return pd.DataFrame(columns=fields)
    old={}
    if prior is not None and not prior.empty and set(('wariant','event_id','symbol','snapshot_utc')).issubset(prior.columns):
        old={tuple(str(r[k]) for k in ('wariant','event_id','symbol','snapshot_utc')):r
             for r in rekordy_audytu_v174(prior, 'prior_audit')}
    res=[]
    for r in rekordy_audytu_v174(now, 'now_audit'):
        key=tuple(str(r[k]) for k in ('wariant','event_id','symbol','snapshot_utc'))
        def h(names):
            values={f:_audit130_norm(r.get(f)) for f in names}
            return _audit130_sha(values) if any(_audit132_presence(v) for v in values.values()) else ''
        ih,oh=h(_AUDIT134_INPUT_FIELDS),h(_AUDIT134_OUTPUT_FIELDS)
        prev=old.get(key,{})
        def compare(new,name):
            if not new or not _audit132_presence(prev.get(name)):return 'NIEPOROWNYWALNE'
            return 'BEZ_ZMIAN' if new==str(prev[name]) else 'ZMIENIONE'
        res.append(dict(zip(('wariant','event_id','symbol','snapshot_utc'),key),
            input_sha256=ih,output_sha256=oh,status_input=compare(ih,'input_sha256'),
            status_output=compare(oh,'output_sha256'),
            uwaga='Odcisk obejmuje dostepne pola (braki jako null), nie surowe swiece OHLCV; zmiana schematu moze zmienic odcisk.'))
    return pd.DataFrame(res,columns=fields)

# V6.136: historical slip is fixed and independent of CURRENT depth
# snapshot; do not present it as an as-of historical estimate.
def audit135_slippage_provenance(now,prior):
    columns=['wariant','event_id','symbol','snapshot_utc','slip_bps_przed',
             'slip_bps_po','porownanie_slip_bps','zrodlo_slip_bps',
             'asof_snapshot_book','ryzyko_lookahead','wplyw_na_wynik_R',
             'zalecenie']
    if now is None or now.empty:return pd.DataFrame(columns=columns)
    keys=('wariant','event_id','symbol','snapshot_utc')
    previous={}
    if prior is not None and not prior.empty and set(keys).issubset(prior.columns):
        previous={tuple(str(r[k]) for k in keys):r for r in rekordy_audytu_v174(prior, 'prior_audit_v174')}
    rows=[]
    for r in rekordy_audytu_v174(now, 'now_audit_v174'):
        k=tuple(str(r.get(f,'')) for f in keys)
        old=previous.get(k,{})
        a,b=old.get('slip_bps'),r.get('slip_bps')
        valid=_audit132_presence(a) and _audit132_presence(b)
        state=('NIEPOROWNYWALNE' if not valid else
               'BEZ_ZMIAN' if _audit135_equal(a,b) else 'ZMIENIONE')
        # Only a stored book timestamp that is <= signal timestamp and a
        # documented capture method could establish as-of validity.
        rows.append(dict(zip(keys,k),slip_bps_przed=a if _audit132_presence(a) else None,
             slip_bps_po=b if _audit132_presence(b) else None,
             porownanie_slip_bps=state,
             zrodlo_slip_bps=HISTORICAL_SLIP_MODEL,
             asof_snapshot_book='NIE_DOTYCZY_MODEL_STALY_OOS',
             ryzyko_lookahead='BRAK_BOOK_LOOKAHEAD_W_KOSZCIE_OOS_V136',
             wplyw_na_wynik_R='NIEUSTALONY_BEZ_PELNEGO_REPLAY',
             zalecenie='Koszt konserwatywny, a nie historyczny spread; interpretowac jako stress-test, bez historycznych snapshotow booka.'))
    return pd.DataFrame(rows,columns=columns)

def test_audyt_v135():
    core={'wariant':'BAZA','event_id':'e','symbol':'BTCUSDT','snapshot_utc':'2026-10-27T00:00:00Z',
          'luka_danych':False,'niejednoznaczne':False,'slip_bps':5.0}
    old=pd.DataFrame([core]); newer=pd.DataFrame([{**core,'luka_danych':'False','niejednoznaczne':'False','slip_bps':'5.0'}])
    details=audit134_details(newer,old)
    for f in ('luka_danych','niejednoznaczne','slip_bps'):
        assert details.loc[details.pole.eq(f),'porownanie'].iloc[0]=='BEZ_ZMIAN',f
    assert _audit135_equal('True',True) and not _audit135_equal('False',True)
    assert not _audit135_equal(False,0) and _audit135_equal('5.00',5)
    assert _audit132_compare({'luka_danych':False},{'luka_danych':'False'},('luka_danych',))=='BEZ_ZMIAN'
    reports=audit135_slippage_provenance(newer,old)
    assert len(reports)==1 and reports.iloc[0].porownanie_slip_bps=='BEZ_ZMIAN'
    assert reports.iloc[0].wplyw_na_wynik_R=='NIEUSTALONY_BEZ_PELNEGO_REPLAY'
    return 'OK (bool CSV, koszt, pochodzenie slippage, ryzyko lookahead)'

def test_audyt_v134():
    d={'wariant':'BAZA','event_id':'1','symbol':'BTCUSDT','snapshot_utc':'2026-10-27T00:00:00Z',
       'r_netto':-1.,'cost_R':0.02,'funding_R':0.,'slip_bps':5.,'source_sha256':'mixed1'}
    prev=pd.DataFrame([d]);curr=pd.DataFrame([{**d,'cost_R':0.03,'source_sha256':'mixed2','r_netto':-1.0005}])
    x=audit134_details(curr,prev)
    row=lambda name:x.loc[x.pole.eq(name)].iloc[0]
    assert row('cost_R').porownanie=='ZMIENIONE' and row('cost_R').przed=='0.02' and row('cost_R').po=='0.03'
    assert row('funding_R').porownanie=='BEZ_ZMIAN'
    assert row('entry').porownanie=='NIEPOROWNYWALNE'
    assert row('source_sha256').interpretacja=='HASH_MIESZANY_NIE_DOWODZI_ZMIANY_OHLCV'
    assert row('ohlcv_snapshot_sha256').porownanie=='NIEPOROWNYWALNE'
    assert row('r_netto').zmiana_R_do_001
    assert audit134_details(prev,prev).loc[lambda q:q.pole.eq('cost_R'),'porownanie'].iloc[0]=='BEZ_ZMIAN'
    assert audit134_checkpoints(curr,prev).iloc[0].status_input=='NIEPOROWNYWALNE'
    return 'OK (wartosci przed/po, koszt, nieporownywalne OHLCV, mixed hash)'

# V6.132: compatibility name for the existing audit pipeline.
audit132_classify=audit131_classify

def test_audyt_v132():
    base={'wariant':'BAZA','event_id':'event','symbol':'BTCUSDT','snapshot_utc':'2026-10-27T00:00:00Z',
          'r_netto':-0.5,'entry':100.,'sl':99.,'tp1':102.,'tp2':103.,'risk_pct':1.,
          'cost_R':0.03,'funding_R':0.,'slip_bps':1.,'exit_utc':'2026-10-27T01:00:00Z',
          'source_sha256':'src','engine_sha256':'engine','setup_type':'CONT_SWEEP',
          'entry_type':'MARKET','side':'LONG','status':'TP','ohlcv_snapshot_sha256':'ohlcv'}
    a=pd.DataFrame([base]); b=pd.DataFrame([{**base,'r_netto':'-0.5','entry':'100.0'}])
    x=audit132_classify(b,a).iloc[0]
    assert x['status131']=='BEZ_ZMIAN' and x['porownanie_poziomow']=='BEZ_ZMIAN'
    legacy={k:v for k,v in base.items() if k not in ('entry','sl','tp1','tp2','risk_pct','cost_R','funding_R','slip_bps','engine_sha256','source_sha256','ohlcv_snapshot_sha256')}
    x=audit132_classify(a,pd.DataFrame([legacy])).iloc[0]
    assert x['status131']=='NIEPOROWNYWALNE_METADANE'
    assert x['porownanie_poziomow']=='NIEPOROWNYWALNE' and x['zmiana_poziomow'] is None
    assert x['porownanie_kodu']=='NIEPOROWNYWALNE'
    x=audit132_classify(pd.DataFrame([{**base,'r_netto':-0.5005}]),pd.DataFrame([legacy])).iloc[0]
    assert x['status131']=='ZMIANA_R' and bool(x['w_tolerancji_R'])
    x=audit132_classify(pd.DataFrame([{**base,'entry':101}]),a).iloc[0]
    assert x['status131']=='ZMIANA_TYLKO_METADANYCH' and x['porownanie_poziomow']=='ZMIENIONE'
    a2=pd.DataFrame([{**base,'source_sha256':float('nan')}]); x=audit132_classify(a,a2).iloc[0]
    assert x['porownanie_zrodla']=='NIEPOROWNYWALNE'
    assert audit132_classify(a,None).iloc[0]['status131']=='BRAK_POROWNANIA'
    assert audit132_classify(a,a).iloc[0]['status131']=='BEZ_ZMIAN'
    return 'OK (checkpoint legacy, trzy stany, zmiany R, liczby z CSV)'

def test_audyt_v131():
    core={'wariant':'BAZA','event_id':'abc','symbol':'BTCUSDT','snapshot_utc':'2026-10-27T00:00:00Z',
          'r_netto':-0.5,'entry':100.,'sl':99.,'tp1':102.,'tp2':103.,'risk_pct':1.,
          'cost_R':0.03,'funding_R':0.,'slip_bps':1.,'exit_utc':'2026-10-27T01:00:00Z',
          'source_sha256':'x','engine_sha256':'e','ohlcv_snapshot_sha256':'fixed_ohlcv_fixture',
          'setup_type':'CONT_SWEEP','entry_type':'MARKET','side':'LONG','status':'tp'}
    a=pd.DataFrame([core]);b=pd.DataFrame([{**core,'status':'closed'}]);
    assert audit131_classify(b,a).iloc[0]['status131']=='ZMIANA_TYLKO_METADANYCH'
    c=pd.DataFrame([{**core,'r_netto':-0.5005}]);v=audit131_classify(c,a).iloc[0]
    assert v['status131']=='ZMIANA_R' and bool(v['w_tolerancji_R'])
    assert audit131_classify(a,a).iloc[0]['status131']=='BEZ_ZMIAN'
    assert audit131_classify(a,None).iloc[0]['status131']=='BRAK_POROWNANIA'
    assert audit131_classify(pd.DataFrame(),a).iloc[0]['status131']=='NIEOBECNY'
    # Two identical calls with the same data and parameters must be bit-for-bit reproducible.
    sample=pd.DataFrame([dict(symbol='BTCUSDT',side='LONG',snapshot_utc='2026-10-27T00:00:00Z',
        event_id='repeat',entry_type='MARKET',setup_type='CONT_SWEEP',status='tp',
        fill_utc='2026-10-27T00:01:00Z',exit_utc='2026-10-27T01:00:00Z',r=1.25,
        score_R=1.0,rr_tp1_net=2.0,cost_R=0.1,grupa='BRAK')])
    first=elliott_portfel_ab(sample,audit_trace=True)
    orders1=[dict(x) for x in elliott_portfel_ab._last_trade_events]
    second=elliott_portfel_ab(sample,audit_trace=True)
    orders2=[dict(x) for x in elliott_portfel_ab._last_trade_events]
    pd.testing.assert_frame_equal(first,second,check_exact=True)
    assert orders1==orders2
    return 'OK (metadata, R, tolerancja, powtarzalnosc silnika)'

def test_audyt_v130():
    base={'wariant':'BAZA','event_id':'a','symbol':'X','snapshot_utc':'2026-10-27T00:00:00Z',
          'r_netto':-1.0,'entry':10,'sl':9,'tp1':12,'tp2':13,'risk_pct':1,
          'cost_R':0.1,'funding_R':0,'slip_bps':1,'source_sha256':'abc','engine_sha256':'eng'}
    a=pd.DataFrame([base]);b=pd.DataFrame([{**base,'r_netto':-1.0005}]);o=audit130_diagnose(b,a)
    assert len(o)==1 and bool(o.iloc[0]['delta_R_w_tolerancji']) and o.iloc[0]['typ_zmiany']=='ZMIANA_R'
    b=pd.DataFrame([{**base,'r_netto':-1.02,'cost_R':0.2}]);o=audit130_diagnose(b,a)
    assert o.iloc[0]['interpretacja']=='ZMIENIONE_PARAMETRY_KOSZTOW'
    b=pd.DataFrame([{**base,'r_netto':-1.02,'entry':11}]);o=audit130_diagnose(b,a)
    assert o.iloc[0]['interpretacja']=='ZMIENIONE_POZIOMY_LUB_RYZYKO'
    assert len(audit130_diagnose(a,None))==1
    return 'OK (tolerancja, koszty, poziomy, brak checkpointu)'

def audyt_portfela_v125(orders, summary, label):
    """Tylko faktycznie wybrane i rozliczone przez silnik portfela; bez selekcji po wyniku."""
    fields=['populacja','wariant','symbol','side','setup_type','entry_type','event_id',
            'snapshot_utc','exit_utc','r_netto','dzien_zamkniecia_utc',*_AUDIT130_SOURCE_FIELDS,
            'source_sha256','engine_sha256','ohlcv_snapshot_sha256','status_zrodla_OHLCV']
    rows=[]
    for o in orders or []:
        r=pd.to_numeric(o.get('r'),errors='coerce')
        dt=pd.to_datetime(o.get('exit'),utc=True,errors='coerce')
        if pd.isna(dt) or pd.isna(r) or not np.isfinite(r):continue
        snap=pd.to_datetime(o.get('snapshot'),utc=True,errors='coerce')
        rows.append({'populacja':label,'wariant':o.get('wariant'),'symbol':o.get('symbol'),
                     'side':o.get('side'),'setup_type':o.get('setup_type'),
                     'entry_type':o.get('entry_type'),'event_id':o.get('event_id'),
                     'snapshot_utc':snap.isoformat() if pd.notna(snap) else '',
                     'exit_utc':dt.isoformat(),'r_netto':float(r),
                     'dzien_zamkniecia_utc':dt.strftime('%Y-%m-%d'),
                     **{k:o.get('audit130_source',{}).get(k) for k in _AUDIT130_SOURCE_FIELDS},
                     'source_sha256':o.get('audit130_source_sha256',''),
                     'engine_sha256':o.get('audit130_engine_sha256',''),
                     'ohlcv_snapshot_sha256':'',
                     'status_zrodla_OHLCV':'BRAK_PELNEGO_SNAPSHOTU_OHLCV'})
    trades=pd.DataFrame(rows,columns=fields)
    stat=[]; day=[]
    if not trades.empty:
        for variant,g in trades.groupby('wariant',sort=True):
            daily=g.groupby('dzien_zamkniecia_utc',sort=True)['r_netto'].agg(['sum','count'])
            for d,v in daily.iterrows():day.append({'populacja':label,'wariant':variant,'dzien_zamkniecia_utc':d,
                                        'transakcje':int(v['count']),'suma_R':round(float(v['sum']),6)})
            total=float(g['r_netto'].sum()); best=float(daily['sum'].max());worst=float(daily['sum'].min())
            stat.append({'populacja':label,'wariant':variant,'transakcje':len(g),'dni_rozliczen':len(daily),
                         'suma_R':round(total,6),'bez_najlepszego_dnia_R':round(total-best,6),
                         'bez_najgorszego_dnia_R':round(total-worst,6),
                         'najlepszy_dzien_udzial_zysku':round(best/total,5) if total>0 else None,
                         'status':'BRAK_POTWIERDZONEJ_PRZEWAGI',
                         'uwaga':'Portfel badawczy: tylko wybrane i rozliczone zlecenia, brak pelnej egzekucji live'})
    summary_cols=['populacja','wariant','transakcje','dni_rozliczen','suma_R','bez_najlepszego_dnia_R',
                  'bez_najgorszego_dnia_R','najlepszy_dzien_udzial_zysku','status','uwaga']
    return trades,pd.DataFrame(day,columns=['populacja','wariant','dzien_zamkniecia_utc','transakcje','suma_R']),pd.DataFrame(stat,columns=summary_cols)


def audyt_par_v125(current, prior=None):
    """Dokladne klucze identyfikujace pary. Nie myli zmian statusu z nowa okazja."""
    cols=['symbol','snapshot_utc','setup_type','side','market_event_id','limit_event_id']
    fields=['symbol','snapshot_utc','setup_type','side','market_event_id','limit_event_id',
            'typ_zmiany','poprzedni_status','obecny_status','poprzednio_ocenialna','obecnie_ocenialna']
    if current is None or current.empty:return pd.DataFrame(columns=fields)
    def idx(t):
        if t is None or t.empty or not set(cols).issubset(t.columns):return {}
        return {tuple(str(row.get(c,'')) for c in cols):row for row in rekordy_audytu_v174(t, 't_audit_v174')}
    cur=idx(current); old=idx(prior)
    records=[]
    for key in sorted(set(cur)|set(old)):
        a=old.get(key); b=cur.get(key)
        as_=str(a.get('status_porownania','')) if a else ''
        bs_=str(b.get('status_porownania','')) if b else ''
        ae=str(a.get('para_ocenialna','')).lower()=='true' if a else False
        be=str(b.get('para_ocenialna','')).lower()=='true' if b else False
        typ=('BRAK_POROWNANIA' if prior is None else 'NOWA_PARA' if a is None else
             'PARA_ZNIKNELA' if b is None else 'ZMIANA_STATUSU' if as_!=bs_ or ae!=be else 'BEZ_ZMIAN')
        records.append(dict(zip(cols,key))|{'typ_zmiany':typ,'poprzedni_status':as_,
                     'obecny_status':bs_,'poprzednio_ocenialna':ae,'obecnie_ocenialna':be})
    return pd.DataFrame(records,columns=fields)



def audyt_zmian_elliott_v127(current, prior=None):
    """Porownuje rozliczone pozycje z poprzednim przebiegiem bez nadpisywania historii.

    Brak w poprzednim checkpoint oznacza NOWE_W_OBECNYM_RAPORCIE, niekoniecznie
    nowo powstala transakcje. Brak w biezacym raporcie to znikniecie z populacji,
    a nie automatycznie anulowanie lub odwrocenie transakcji.
    """
    columns=['wariant','event_id','symbol','snapshot_utc','exit_utc',
             'poprzedni_R','obecny_R','zmiana_R','typ_zmiany']
    ident=['wariant','event_id','symbol','snapshot_utc']
    if current is None:current=pd.DataFrame()
    def build(df):
        if df is None or df.empty or not set(ident+['r_netto']).issubset(df.columns):return {}
        v={}
        for r in rekordy_audytu_v174(df, 'df_audit_v174'):
            k=tuple(str(r.get(f,'')) for f in ident)
            if k in v:raise ValueError('Zduplikowany identyfikator transakcji Elliott w audycie')
            v[k]=r
        return v
    a=build(prior);b=build(current)
    records=[]
    for key in sorted(set(a)|set(b)):
        old=a.get(key);now=b.get(key)
        pre=float(old['r_netto']) if old else None
        cur=float(now['r_netto']) if now else None
        if prior is None:typ='BRAK_POROWNANIA'
        elif old is None:typ='NOWA_W_RAPORCIE'
        elif now is None:typ='ZALICZONA_W_POPRZEDNIM_RAPORCIE_BRAK_OBECNIE'
        elif abs(pre-cur)>1e-8 or str(old.get('exit_utc',''))!=str(now.get('exit_utc','')):typ='ZMIANA_ROZLICZENIA'
        else:typ='BEZ_ZMIAN'
        records.append(dict(zip(ident,key),exit_utc=(now or old).get('exit_utc',''),
            poprzedni_R=pre,obecny_R=cur,
            zmiana_R=(cur or 0)-(pre or 0),typ_zmiany=typ))
    return pd.DataFrame(records,columns=columns)


def audyt_przyczyn_elliott_v129(changes, current, prior=None):
    """Diagnostyka tylko na podstawie dostepnych checkpointow, bez zgadywania przyczyn.

    Zmiana R nie dowodzi zmiany swiec, prowizji ani danych historycznych.
    By ustalic zrodlo nalezy zapisac identyczne wejsciowe OHLCV i koszty w obu przebiegach.
    """
    columns=['wariant','event_id','symbol','snapshot_utc','typ_zmiany',
             'exit_poprzednio','exit_teraz','poprzedni_R','obecny_R','delta_R',
             'zmiana_czasu_wyjscia','zmiana_wyniku_R','zmiana_setup_type',
             'zmiana_entry_type','klasyfikacja_obserwacji','przyczyna_zrodlowa',
             'dane_potrzebne_do_wyjasnienia']
    ident=('wariant','event_id','symbol','snapshot_utc')
    def lookup(df):
        if df is None or df.empty:return {}
        if not set(ident).issubset(df.columns):return {}
        result={}
        for row in rekordy_audytu_v174(df, 'df_audit_v174'):
            key=tuple(str(row.get(x,'')) for x in ident)
            if key in result:raise ValueError('V6.129: duplikat klucza transakcji')
            result[key]=row
        return result
    old=lookup(prior);now=lookup(current)
    records=[]
    for d in rekordy_audytu_v174(changes, 'changes_audit_v174') if changes is not None else []:
        key=tuple(str(d.get(x,'')) for x in ident)
        a=old.get(key);b=now.get(key)
        def unequal(field):
            if a is None or b is None or field not in a or field not in b:return None
            x=a[field];y=b[field]
            if pd.isna(x) and pd.isna(y):return False
            return str(x)!=str(y)
        exit_changed=unequal('exit_utc')
        x=pd.to_numeric(a.get('r_netto') if a else None,errors='coerce')
        y=pd.to_numeric(b.get('r_netto') if b else None,errors='coerce')
        r_changed=(abs(float(x)-float(y))>1e-8) if pd.notna(x) and pd.notna(y) else None
        typ=d.get('typ_zmiany','')
        if typ=='ZMIANA_ROZLICZENIA':
            classification=('R_I_CZAS_WYJSCIA' if r_changed and exit_changed else
                            'TYLKO_R' if r_changed else 'TYLKO_CZAS_WYJSCIA' if exit_changed else
                            'INNE_POLE_LUB_PRECYZJA')
        else:classification=str(typ)
        records.append({**{k:d.get(k,'') for k in ident},'typ_zmiany':typ,
          'exit_poprzednio':a.get('exit_utc','') if a else '',
          'exit_teraz':b.get('exit_utc','') if b else '',
          'poprzedni_R':float(x) if pd.notna(x) else None,
          'obecny_R':float(y) if pd.notna(y) else None,
          'delta_R':float(y-x) if pd.notna(x) and pd.notna(y) else None,
          'zmiana_czasu_wyjscia':exit_changed,'zmiana_wyniku_R':r_changed,
          'zmiana_setup_type':unequal('setup_type'),
          'zmiana_entry_type':unequal('entry_type'),
          'klasyfikacja_obserwacji':classification,
          'przyczyna_zrodlowa':('NIEUSTALONA_BRAK_SNAPSHOTOW_DANYCH' if typ=='ZMIANA_ROZLICZENIA'
                                 else 'NIE_DOTYCZY'),
          'dane_potrzebne_do_wyjasnienia':('OHLCV_z_obu_przebiegow;parametry_kosztow;entry_SL_TP;wersja_silnika'
                                         if typ=='ZMIANA_ROZLICZENIA' else '')})
    return pd.DataFrame(records,columns=columns)


def test_audyt_przyczyn_v129():
    base={'wariant':'ELLIOTT','event_id':'a','symbol':'TEST','snapshot_utc':'2026-10-27T01:00:00Z',
          'exit_utc':'2026-10-27T02:00:00Z','r_netto':-0.5,'setup_type':'X','entry_type':'LIMIT'}
    changed={**base,'r_netto':-0.6}
    a=pd.DataFrame([base]);b=pd.DataFrame([changed]);diff=audyt_zmian_elliott_v127(b,a)
    out=audyt_przyczyn_elliott_v129(diff,b,a)
    assert len(out)==1 and out.iloc[0]['klasyfikacja_obserwacji']=='TYLKO_R'
    assert out.iloc[0]['przyczyna_zrodlowa']=='NIEUSTALONA_BRAK_SNAPSHOTOW_DANYCH'
    assert abs(out.iloc[0]['delta_R']+0.1)<1e-9
    c=pd.DataFrame([{**base,'exit_utc':'2026-10-27T03:00:00Z'}])
    co=audyt_przyczyn_elliott_v129(audyt_zmian_elliott_v127(c,a),c,a)
    assert co.iloc[0]['klasyfikacja_obserwacji']=='TYLKO_CZAS_WYJSCIA'
    return True


def kontrola_sum_elliott_v127(portfolio_summary, transactions, tol=0.0001):
    """Fail-visible diagnostyka spójnosci z silnikiem; brak porownania != PASS.

    Uzywamy niezaokraglonych R z transakcji. Raport z algorytmu zawiera
    zaokraglone wartosci, stad tolerancja 0.0001 R na transakcje.
    """
    columns=['wariant','silnik_transakcje','audyt_transakcje','silnik_suma_R',
             'audyt_suma_R','roznica_R','status']
    rows=[]
    if portfolio_summary is None or portfolio_summary.empty or not {'wariant','suma_R','transakcje'}.issubset(portfolio_summary.columns):
        return pd.DataFrame([{'wariant':'WSZYSTKIE','status':'BRAK_RAPORTU_SILNIKA'}],columns=columns)
    for _,r in portfolio_summary.iterrows():
        typ=str(r['wariant'])
        group=(transactions.loc[transactions['wariant'].astype(str).eq(typ)]
               if transactions is not None and not transactions.empty else pd.DataFrame())
        count=int(len(group)); total=float(pd.to_numeric(group['r_netto'],errors='coerce').sum()) if count else 0.
        expected=float(r['suma_R']); n=int(r['transakcje'])
        delta=round(total-expected,8)
        ok=count==n and abs(delta)<=tol*max(1,n)
        rows.append({'wariant':typ,'silnik_transakcje':n,'audyt_transakcje':count,
                     'silnik_suma_R':expected,'audyt_suma_R':round(total,6),
                     'roznica_R':delta,'status':'ZGODNE' if ok else 'NIEZGODNE'})
    return pd.DataFrame(rows,columns=columns)


def test_audyt_v125():
    dt=pd.Timestamp('2026-10-28T12:00:00Z')
    t,d,s=audyt_portfela_v125([{'wariant':'MARKET','symbol':'TEST','side':'LONG','setup_type':'TEST',
             'entry_type':'MARKET','event_id':'x','snapshot':dt,'exit':dt,'r':1.0}],None,'TEST')
    assert len(t)==1 and s.iloc[0]['suma_R']==1.0 and len(d)==1
    row=dict(symbol='TEST',snapshot_utc='2026-10-28T12:00:00Z',setup_type='TEST',side='LONG',
             market_event_id='x',limit_event_id='y',status_porownania='CENZUROWANE',para_ocenialna=False)
    old=pd.DataFrame([row]); now=pd.DataFrame([dict(row,status_porownania='DECYZJE_OCENIALNE',para_ocenialna=True)])
    assert audyt_par_v125(now,old).iloc[0]['typ_zmiany']=='ZMIANA_STATUSU'
    assert audyt_par_v125(now,None).iloc[0]['typ_zmiany']=='BRAK_POROWNANIA'


def test_raport_decyzji_wejsc_oos():
    base={'symbol':'AAA','snapshot_utc':'2026-10-28T00:00:00Z','setup_type':'CONT_SWEEP',
          'side':'LONG','filtr':'','elliott_vs_setup':'BRAK','wait_h':1}
    a=pd.DataFrame([dict(base,entry_type='MARKET',status='TP1',r=1.0,event_id='ma',
                         fill_utc='2026-10-28T00:05:00Z',exit_utc='2026-10-28T00:30:00Z'),
                    dict(base,entry_type='LIMIT',status='niewypelniony',r=None,event_id='li',
                         fill_utc=None,exit_utc=None)])
    summary,detail,port=raport_decyzji_wejsc_oos(a)
    assert len(detail)==1 and bool(detail.iloc[0]['para_ocenialna'])
    assert abs(float(detail.iloc[0]['roznica_R'])-1.0)<1e-8
    assert abs(float(summary.iloc[1]['suma_roznic_R'])-0.95)<1e-8
    assert len(port)==12
    a.loc[1,'status']='oczekuje'
    summary,detail,port=raport_decyzji_wejsc_oos(a)
    assert not bool(detail.iloc[0]['para_ocenialna']) and int(summary.iloc[0]['pary_cenzurowane'])==1
    return 'PASS'


def test_raport_parowany_wejsc_oos():
    base={'symbol':'AAA','snapshot_utc':'2026-10-28T00:00:00Z','setup_type':'CONT_SWEEP',
          'side':'LONG','filtr':'','elliott_abc_status':'BRAK'}
    df=pd.DataFrame([dict(base,entry_type='MARKET',status='TP1',r=1.0,event_id='m'),
                     dict(base,entry_type='LIMIT',status='SL',r=-1.0,event_id='l'),
                     dict(base,symbol='BBB',entry_type='MARKET',status='SL',r=-1.0,event_id='b')])
    a,b,c=raport_parowany_wejsc_oos(df)
    q=a[(a['strategia']=='WSZYSTKIE') & a['koszt_dodatkowy_R'].eq(0)].iloc[0]
    assert len(b)==1 and q['pary_z_oboma_rozliczeniami']==1 and abs(q['suma_roznic_R']-2)<1e-9
    assert len(c)==2
    return 'PASS'


def test_raport_trzech_wejsc_oos():
    t=pd.DataFrame([{'snapshot_utc':'2026-10-28T01:00:00Z','symbol':'AAA',
      'entry_type':'MARKET','side':'LONG','status':'TP1','r':1.1,'filtr':'',
      'okazja_id':'a','exit_utc':'2026-10-28T03:00:00Z',
      'elliott_abc_status':'ABC_POTENCJALNIE_ZAKONCZONE',
      'elliott_abc_kierunek':'LONG','elliott_abc_odleglosc_atr':1.2},
      {'snapshot_utc':'2026-10-28T02:00:00Z','symbol':'BBB',
      'entry_type':'LIMIT','side':'SHORT','status':'SL','r':-1.0,'filtr':'',
      'okazja_id':'b','exit_utc':'2026-10-28T04:00:00Z',
      'elliott_abc_status':'ABC_POTENCJALNIE_ZAKONCZONE',
      'elliott_abc_kierunek':'SHORT','elliott_abc_odleglosc_atr':3.4}])
    a,b=raport_trzech_wejsc_oos(t)
    assert len(a)==9 and len(b)==3
    assert int(a.query("kategoria=='ELLIOTT_ABC_SWIEZE' and dodatkowy_koszt_R==0")['rozliczone_wiersze'].iloc[0])==1
    assert abs(float(a.query("kategoria=='MARKET' and dodatkowy_koszt_R==0.10")['suma_R_netto'].iloc[0])-1.0)<1e-8
    return 'PASS'


def portfolio_three_way(df):
    """V6.89: trzy rankingi przy tej samej populacji, kosztach i rezerwacji slotow.
    Filtry AKTUALNE_REGULY sa retrospektywne i NIE stanowia walk-forward.
    OOS: tylko kandydatury od daty granicznej; niezalezna proba jest jeszcze mala.
    """
    cols=['populacja','okres','miejsca_kierunek','dodatkowy_koszt_R','kolejnosc_remisow',
          'wariant','zlecenia_wybrane','transakcje','niewypelnione','anulowane','w_toku',
          'suma_R','sredni_R_netto','win_rate_pct','max_drawdown_R','przewaga_vs_BAZA_R','wniosek','uwaga']
    if df is None or df.empty or 'snapshot_utc' not in df:
        return pd.DataFrame(columns=cols)
    x=df.copy()
    x['_t']=pd.to_datetime(x['snapshot_utc'],utc=True,errors='coerce')
    x=x[x['_t'].notna()].copy()
    if x.empty: return pd.DataFrame(columns=cols)
    if {'setup_type','side','entry_type'}.issubset(x.columns):
        excluded=[bool(typ_wylaczony(str(t),s,e)) for t,s,e in zip(x['setup_type'],x['side'],x['entry_type'])]
    else: excluded=[False]*len(x)
    populations={'FILTRY_Z_CHWILI_SYGNALU':x,
                 'AKTUALNE_REGULY_RETROSPEKTYWNIE':x.loc[~np.asarray(excluded,dtype=bool)]}
    cut=pd.Timestamp(MARKET_AB_OOS_OD)
    names=('BAZA','MARKET_PIERWSZE','ELLIOTT_HIPOTEZA')
    rows=[]
    for pop_name, pop in populations.items():
        for period, subset in [('CALOSC',pop),('OOS_OD_2026_10_27',pop.loc[pop['_t']>=cut])]:
            data=subset.drop(columns=['_t'])
            for slots in (2,3):
                for cost in (0.0,0.05,0.10):
                    for reverse in (False,True):
                        cutoff=data['snapshot_utc'].pipe(pd.to_datetime,utc=True,errors='coerce').max() if period.startswith('OOS_') and len(data) else None
                        tab=elliott_portfel_ab(data,miejsca=slots,extra_cost_R=cost,
                                               tie_reverse=reverse,warianty=names,asof_utc=cutoff)
                        if tab.empty: continue
                        baseline=tab.loc[tab['wariant']=='BAZA','suma_R']
                        if baseline.empty: continue
                        baseline=float(baseline.iloc[0])
                        for r in tab.to_dict('records'):
                            total=float(r['suma_R']); trades=int(r['transakcje'])
                            # Brak rozliczen nie jest dowodem wyniku zero.
                            verdict='BRAK_ROZLICZEN' if trades==0 else (
                                'NIERENTOWNY' if total<=0 else 'DODATNI_BEZ_POTWIERDZENIA_OOS')
                            rows.append({**{k:r.get(k) for k in cols if k in r},
                                'populacja':pop_name,'okres':period,'miejsca_kierunek':slots,
                                'dodatkowy_koszt_R':cost,'kolejnosc_remisow':'odwrotna' if reverse else 'normalna',
                                'przewaga_vs_BAZA_R':round(total-baseline,4), 'wniosek':verdict,
                                'uwaga':('RETROSPEKTYWNY WYBOR REGUL; NIE OOS walk-forward' if pop_name.startswith('AKTUALNE_')
                                         else 'BADAWCZA SYMULACJA; ten sam zbior sygnalow, nie pelna egzekucja live')})
    return pd.DataFrame(rows,columns=cols)



SYMBOL_STABILITY_COLS=['populacja','okres','miejsca_kierunek','dodatkowy_koszt_R','kolejnosc_remisow','wariant','pominiety_symbol','suma_R_pelna','suma_R_bez_symbolu','przewaga_pelna_R','przewaga_bez_symbolu_R','odwrocenie_przewagi','uwaga']

def portfolio_three_way_daily_stability(df):
    """V6.90: te same wybrane zlecenia, PnL wg DATY ZAMKNIECIA.
    Leave-one-day-out usuwa rozliczenia z dnia, NIE wykonuje ponownego doboru
    pozycji po usunieciu dnia (nie udaje odrebnego backtestu walk-forward).
    """
    daily_cols=['populacja','okres','miejsca_kierunek','dodatkowy_koszt_R','kolejnosc_remisow',
                'dzien_zamkniecia_utc','wariant','transakcje','suma_R','przewaga_vs_BAZA_R']
    loo_cols=['populacja','okres','miejsca_kierunek','dodatkowy_koszt_R','kolejnosc_remisow',
              'pominiety_dzien_utc','wariant','transakcje_pelne','suma_R_pelne',
              'suma_R_bez_dnia','przewaga_vs_BAZA_pelna_R','przewaga_vs_BAZA_bez_dnia_R',
              'zmiana_przewagi_R','odwrocenie_przewagi','uwaga']
    summary_cols=['populacja','okres','miejsca_kierunek','dodatkowy_koszt_R','kolejnosc_remisow',
                  'wariant','transakcje','suma_R','przewaga_vs_BAZA_R','dni_z_rozliczeniami',
                  'dni_z_przewaga','dni_z_przegrana','dni_remis','dni_odwracajace_przewage',
                  'najgorsza_przewaga_po_pominieciu_R','status','uwaga']
    portfolio_three_way_daily_stability._symbol_rows=pd.DataFrame(columns=SYMBOL_STABILITY_COLS)
    if df is None or df.empty or 'snapshot_utc' not in df.columns:
        return (pd.DataFrame(columns=daily_cols),pd.DataFrame(columns=loo_cols),pd.DataFrame(columns=summary_cols))
    x=df.copy(); x['_t']=pd.to_datetime(x['snapshot_utc'],utc=True,errors='coerce')
    x=x.loc[x['_t'].notna()].copy()
    if x.empty:
        return (pd.DataFrame(columns=daily_cols),pd.DataFrame(columns=loo_cols),pd.DataFrame(columns=summary_cols))
    if {'setup_type','side','entry_type'}.issubset(x.columns):
        disabled=[bool(typ_wylaczony(str(t),side,entry)) for t,side,entry in zip(x['setup_type'],x['side'],x['entry_type'])]
    else: disabled=[False]*len(x)
    pops={'FILTRY_Z_CHWILI_SYGNALU':x,'AKTUALNE_REGULY_RETROSPEKTYWNIE':x.loc[~np.asarray(disabled,dtype=bool)]}
    variants=('BAZA','MARKET_PIERWSZE','ELLIOTT_HIPOTEZA')
    rows_daily=[]; rows_loo=[]; rows_summary=[]; symbol_rows=[]
    for popname, pop in pops.items():
        for period, data in [('CALOSC',pop),('OOS_OD_2026_10_27',pop.loc[pop['_t']>=pd.Timestamp(MARKET_AB_OOS_OD)])]:
            for slots in (2,3):
                for cost in (0.0,0.05,0.10):
                    for reverse in (False,True):
                        cutoff=data['_t'].max() if period.startswith('OOS_') and len(data) else None
                        tab=elliott_portfel_ab(data.drop(columns=['_t']),miejsca=slots,extra_cost_R=cost,
                                              tie_reverse=reverse,warianty=variants,asof_utc=cutoff)
                        events=getattr(elliott_portfel_ab,'_last_trade_events',[])
                        if tab.empty: continue
                        meta={'populacja':popname,'okres':period,'miejsca_kierunek':slots,
                              'dodatkowy_koszt_R':cost,'kolejnosc_remisow':'odwrotna' if reverse else 'normalna'}
                        totals={v:float(tab.loc[tab.wariant==v,'suma_R'].iloc[0]) for v in variants}
                        counts={v:int(tab.loc[tab.wariant==v,'transakcje'].iloc[0]) for v in variants}
                        per_day={v:{} for v in variants}
                        n_day={v:{} for v in variants}
                        for e in events:
                            v=e['wariant']; stamp=pd.to_datetime(e['exit'],utc=True,errors='coerce')
                            if v not in per_day or pd.isna(stamp) or e['r'] is None: continue
                            day=stamp.strftime('%Y-%m-%d')
                            per_day[v][day]=per_day[v].get(day,0.)+float(e['r'])
                            n_day[v][day]=n_day[v].get(day,0)+1
                        days=sorted(set().union(*(set(d) for d in per_day.values())))
                        base=totals['BAZA']
                        # Leave-one-symbol-out: usuwa PnL bez ponownej selekcji zlecen.
                        per_symbol={v:{} for v in variants}
                        for e in events:
                            v=e['wariant']; sym=str(e.get('symbol',''))
                            if v in per_symbol and sym and e['r'] is not None:
                                per_symbol[v][sym]=per_symbol[v].get(sym,0.)+float(e['r'])
                        for sym in sorted(set().union(*(set(d) for d in per_symbol.values()))):
                            for v in variants:
                                edge=totals[v]-base
                                without=(totals[v]-per_symbol[v].get(sym,0.))-(base-per_symbol['BAZA'].get(sym,0.))
                                symbol_rows.append({**meta,'wariant':v,'pominiety_symbol':sym,
                                    'suma_R_pelna':round(totals[v],4),
                                    'suma_R_bez_symbolu':round(totals[v]-per_symbol[v].get(sym,0.),4),
                                    'przewaga_pelna_R':round(edge,4),'przewaga_bez_symbolu_R':round(without,4),
                                    'odwrocenie_przewagi':bool(v!='BAZA' and edge>0 and without<=0),
                                    'uwaga':'Bez ponownego doboru portfela; usunieto tylko PnL symbolu'})
                        for day in days:
                            for v in variants:
                                rows_daily.append({**meta,'dzien_zamkniecia_utc':day,'wariant':v,
                                                   'transakcje':n_day[v].get(day,0),
                                                   'suma_R':round(per_day[v].get(day,0.),4),
                                                   'przewaga_vs_BAZA_R':round(per_day[v].get(day,0.)-per_day['BAZA'].get(day,0.),4)})
                                full_edge=totals[v]-base
                                leave_edge=full_edge-(per_day[v].get(day,0.)-per_day['BAZA'].get(day,0.))
                                rows_loo.append({**meta,'pominiety_dzien_utc':day,'wariant':v,
                                    'transakcje_pelne':counts[v],'suma_R_pelne':round(totals[v],4),
                                    'suma_R_bez_dnia':round(totals[v]-per_day[v].get(day,0.),4),
                                    'przewaga_vs_BAZA_pelna_R':round(full_edge,4),
                                    'przewaga_vs_BAZA_bez_dnia_R':round(leave_edge,4),
                                    'zmiana_przewagi_R':round(leave_edge-full_edge,4),
                                    'odwrocenie_przewagi':bool(v!='BAZA' and full_edge>0 and leave_edge<=0),
                                    'uwaga':'Tylko usuniecie PnL wg dnia zamkniecia, bez ponownej selekcji; NIE walk-forward'})
                        for v in variants:
                            edge=totals[v]-base
                            changes=[per_day[v].get(day,0.)-per_day['BAZA'].get(day,0.) for day in days]
                            leave=[edge-d for d in changes]
                            flips=sum(edge>0 and val<=0 for val in leave) if v!='BAZA' else 0
                            # OOS: mala proba nie moze dostac statusu potwierdzenia.
                            status=('BRAK_ROZLICZEN' if counts[v]==0 else
                                    'NIEWYSTARCZAJACA_PROBA_OOS' if period.startswith('OOS_') and (counts[v]<20 or len(days)<5) else
                                    'BRAK_DODATNIEJ_RENTOWNOSCI' if totals[v]<=0 else
                                    'NIESTABILNA_PRZEWAGA' if v!='BAZA' and (edge<=0 or flips>0) else
                                    'DODATNI_WYNIK_BADAWCZY_BEZ_POTWIERDZENIA')
                            rows_summary.append({**meta,'wariant':v,'transakcje':counts[v],
                                'suma_R':round(totals[v],4),'przewaga_vs_BAZA_R':round(edge,4),
                                'dni_z_rozliczeniami':sum(n_day[v].get(day,0)>0 for day in days),
                                'dni_z_przewaga':sum(z>1e-8 for z in changes) if v!='BAZA' else 0,
                                'dni_z_przegrana':sum(z<-1e-8 for z in changes) if v!='BAZA' else 0,
                                'dni_remis':sum(abs(z)<=1e-8 for z in changes) if v!='BAZA' else len(days),
                                'dni_odwracajace_przewage':flips,
                                'najgorsza_przewaga_po_pominieciu_R':round(min(leave),4) if leave else None,
                                'status':status,
                                'uwaga':'RETROSPEKTYWNE REGULY: NIE OOS walk-forward' if popname.startswith('AKTUALNE_') else
                                        'Badawcza symulacja; LOO po dniach zamkniecia, bez ponownego wyboru zlecen'})
    portfolio_three_way_daily_stability._symbol_rows=pd.DataFrame(symbol_rows,columns=SYMBOL_STABILITY_COLS)
    return (pd.DataFrame(rows_daily,columns=daily_cols),
            pd.DataFrame(rows_loo,columns=loo_cols),pd.DataFrame(rows_summary,columns=summary_cols))



def portfolio_three_way_verdict(stability):
    cols=['populacja','okres','miejsca_kierunek','dodatkowy_koszt_R','kolejnosc_remisow',
          'wariant','transakcje','suma_R','przewaga_vs_BAZA_R','status','uwaga']
    if stability is None or stability.empty:
        return pd.DataFrame(columns=cols)
    x=stability.copy()
    x['status']=x['status'].fillna('BRAK_POTWIERDZENIA')
    x['uwaga']=x['uwaga'].astype(str)+'; nie jest zgoda na handel'
    return x.reindex(columns=cols)

def test_portfolio_v691():
    # Zamkniecie znane dopiero po chwili odciecia NIE moze wejsc do R.
    rows=[{'snapshot_utc':'2026-10-27T10:00:00Z','symbol':'A','side':'LONG',
           'status':'TP1','r':2.0,'entry_type':'MARKET','event_id':'a',
           'fill_utc':'2026-10-27T10:01:00Z','exit_utc':'2026-10-27T12:00:00Z',
           'wait_h':1,'score_R':1,'rr_tp1_net':2,'cost_R':0.01,'elliott_vs_setup':'BRAK'},
          {'snapshot_utc':'2026-10-27T11:00:00Z','symbol':'B','side':'LONG',
           'status':'SL','r':-1.0,'entry_type':'MARKET','event_id':'b',
           'fill_utc':'2026-10-27T11:01:00Z','exit_utc':'2026-10-27T11:03:00Z',
           'wait_h':1,'score_R':1,'rr_tp1_net':2,'cost_R':0.01,'elliott_vs_setup':'BRAK'}]
    a=elliott_portfel_ab(pd.DataFrame(rows),warianty=('BAZA',),asof_utc='2026-10-27T11:30:00Z')
    assert a.iloc[0]['transakcje']==1 and abs(a.iloc[0]['suma_R']+1)<1e-7,a
    d,l,st=portfolio_three_way_daily_stability(pd.DataFrame(rows))
    sy=portfolio_three_way_daily_stability._symbol_rows
    assert not sy.empty and {'A','B'}.issubset(set(sy.pominiety_symbol)),sy
    assert not portfolio_three_way_verdict(st).empty
    # LIMIT wypelniony po odcieciu: nie uznajemy przyszlego fill ani wyniku.
    lim=pd.DataFrame([{**rows[0], 'entry_type':'LIMIT','fill_utc':'2026-10-27T12:01:00Z',
                       'exit_utc':'2026-10-27T13:00:00Z','wait_h':4}])
    z=elliott_portfel_ab(lim,warianty=('BAZA',),asof_utc='2026-10-27T11:30:00Z')
    assert z.iloc[0]['transakcje']==0,z
    return 'OK (as-of MARKET/LIMIT, symbol LOO, verdict)'

def test_portfolio_three_way_stability():
    empty=portfolio_three_way_daily_stability(pd.DataFrame())
    assert len(empty)==3 and all(a.empty for a in empty)
    rows=[]
    for i,r in enumerate((1.2,-0.8,0.6)):
        day=27+i
        rows.append({'snapshot_utc':f'2026-10-{day}T01:00:00Z','symbol':f'X{i}',
                     'side':'LONG','status':'TP1' if r>0 else 'SL','r':r,
                     'entry_type':'MARKET','event_id':f'X{i}','fill_utc':f'2026-10-{day}T01:05:00Z',
                     'exit_utc':f'2026-10-{day}T02:00:00Z','wait_h':1,
                     'score_R':1,'rr_tp1_net':2,'cost_R':0.01,'elliott_vs_setup':'BRAK','setup_type':'ODBICIE'})
    daily,loo,stats=portfolio_three_way_daily_stability(pd.DataFrame(rows))
    q=stats[(stats.populacja=='FILTRY_Z_CHWILI_SYGNALU') & (stats.okres=='CALOSC') &
            (stats.miejsca_kierunek==3)&(stats.dodatkowy_koszt_R==0.05)&(stats.kolejnosc_remisow=='normalna')]
    assert set(q.wariant)=={'BAZA','MARKET_PIERWSZE','ELLIOTT_HIPOTEZA'}
    assert all(abs(v-0.85)<1e-6 for v in q.suma_R)
    oos=stats[(stats.populacja=='FILTRY_Z_CHWILI_SYGNALU') & (stats.okres=='OOS_OD_2026_10_27')]
    assert all(v in ('NIEWYSTARCZAJACA_PROBA_OOS','BRAK_ROZLICZEN') for v in oos.status)
    z=loo[(loo.populacja=='FILTRY_Z_CHWILI_SYGNALU')&(loo.okres=='CALOSC') &
          (loo.wariant=='BAZA') & (loo.miejsca_kierunek==3)& (loo.dodatkowy_koszt_R==0.05)&
          (loo.kolejnosc_remisow=='normalna')]
    assert len(z)==3
    assert abs(float(z[z.pominiety_dzien_utc=='2026-10-27'].iloc[0].suma_R_bez_dnia)-(-0.3))<1e-5
    return 'OK (wspolne dni exit, leave-one-day-out, stres kosztowy, fail-closed OOS)'


def test_portfolio_three_way():
    """Test porownywalnosci, kosztow, proby pustej i wariantow bez zmian tradingu."""
    cols=['snapshot_utc','symbol','side','status','r','entry_type','event_id','fill_utc','exit_utc',
          'wait_h','score_R','rr_tp1_net','cost_R','elliott_vs_setup','setup_type']
    assert portfolio_three_way(pd.DataFrame(columns=cols)).empty
    rows=[]
    for sym, entry, score, rv in [('L','LIMIT',2.0,None),('M','MARKET',1.0,1.25)]:
        rows.append({'snapshot_utc':'2026-10-27T01:00:00Z','symbol':sym,'side':'LONG',
                     'status':'niewypelniony' if rv is None else 'TP1','r':rv,
                     'entry_type':entry,'event_id':sym,'fill_utc':None if rv is None else '2026-10-27T01:05:00Z',
                     'exit_utc':None if rv is None else '2026-10-27T01:15:00Z',
                     'wait_h':2,'score_R':score,'rr_tp1_net':2.0,'cost_R':0.01,
                     'elliott_vs_setup':'BRAK','setup_type':'CONT_SWEEP'})
    out=portfolio_three_way(pd.DataFrame(rows))
    assert set(out['wariant'])=={'BAZA','MARKET_PIERWSZE','ELLIOTT_HIPOTEZA'}
    a=out[(out.populacja=='FILTRY_Z_CHWILI_SYGNALU') & (out.okres=='OOS_OD_2026_10_27') &
          (out.miejsca_kierunek==2)&(out.kolejnosc_remisow=='normalna')]
    for variant in ('BAZA','MARKET_PIERWSZE','ELLIOTT_HIPOTEZA'):
        z=a[a.wariant==variant].sort_values('dodatkowy_koszt_R')
        assert list(z['transakcje'])==[0,0,0]  # exit dopiero po ostatnim snapshot
        full=out[(out.populacja=='FILTRY_Z_CHWILI_SYGNALU') & (out.okres=='CALOSC') &
                 (out.miejsca_kierunek==2)&(out.kolejnosc_remisow=='normalna') & (out.wariant==variant)].sort_values('dodatkowy_koszt_R')
        assert list(full['transakcje'])==[1,1,1]
        assert abs(float(full.iloc[0]['suma_R'])-1.25)<1e-5
        assert abs(float(full.iloc[2]['suma_R'])-1.15)<1e-5
    return 'OK (wspolna populacja, OOS, 3 rankingi, koszt, pusty zbior)'


def elliott_ab_dzienny(df, miejsca=3, poczatek=None):
    """Badawczy wynik wg dnia zamkniecia. Rekonstrukcja identycznej selekcji
    BAZA/Elliott, bez uzywania wynikow przy wyborze. Zero w dniu bez zamkniec.
    """
    cols=['wariant','dzien_utc','transakcje','suma_R','suma_R_narastajaco',
          'dzien_dodatni','uwaga']
    if df is None or df.empty:
        return pd.DataFrame(columns=cols)
    # Dzienny zestaw wymaga wydarzen exit. Zapisane w funkcji A/B przez
    # wewnetrzny collector (bez powielania algorytmu doboru).
    _, _, exits = elliott_portfel_ab(df,miejsca=miejsca,return_weekly=True,
                                     return_exit_weekly=True)
    # Dla dokladnego rozkladu dziennego uzyj indywidualnych wyborow, a nie
    # agregatu tygodniowego; odtworzenie selekcji realizuje wspolny silnik.
    trades=getattr(elliott_portfel_ab, '_last_trade_events', [])
    rows=[]
    if not trades: return pd.DataFrame(columns=cols)
    dat=pd.DataFrame(trades)
    dat['day']=pd.to_datetime(dat['exit'],utc=True,errors='coerce').dt.floor('D')
    dat=dat[dat['day'].notna()]
    if dat.empty: return pd.DataFrame(columns=cols)
    first=pd.to_datetime(poczatek,utc=True,errors='coerce') if poczatek else pd.to_datetime(df['snapshot_utc'],utc=True,errors='coerce').min()
    first=first.floor('D') if pd.notna(first) else dat['day'].min()
    last=dat['day'].max()
    for variant in ('BAZA','ELLIOTT_HIPOTEZA'):
        subset=dat[dat['wariant']==variant]
        byday=subset.groupby('day')['r'].agg(['sum','count'])
        total=0.0
        for day in pd.date_range(first,last,freq='D',tz='UTC'):
            if day in byday.index:
                value=float(byday.loc[day,'sum']); cnt=int(byday.loc[day,'count'])
            else: value=0.0;cnt=0
            total+=value
            rows.append({'wariant':variant,'dzien_utc':day.strftime('%Y-%m-%d'),
                'transakcje':cnt,'suma_R':round(value,4),
                'suma_R_narastajaco':round(total,4),'dzien_dodatni':bool(value>0),
                'uwaga':'Dzien zamkniecia; zero gdy bez transakcji; badawcze A/B'})
    return pd.DataFrame(rows,columns=cols)


def elliott_oos_dzienna_jakosc(daily, min_dni=10, max_udzial=0.50):
    """Ocenia czy wynik OOS zalezy od pojedynczej sesji.
    Nie przyznaje prawa do handlu nawet dla dobrej koncentracji.
    """
    cols=['wariant','dni_kalendarzowe','dni_z_transakcjami','dni_dodatnie',
          'suma_R','najlepszy_dzien_R','najgorszy_dzien_R',
          'udzial_najlepszego_dnia_w_zyskach_pct','status_jakosci','uwaga']
    if daily is None or daily.empty: return pd.DataFrame(columns=cols)
    rows=[]
    for variant,g in daily.groupby('wariant'):
        v=pd.to_numeric(g['suma_R'],errors='coerce').fillna(0)
        profits=float(v.clip(lower=0).sum())
        best=float(v.max()); total=float(v.sum())
        share=best/profits if profits>0 and best>0 else None
        trade_days=int((pd.to_numeric(g['transakcje'],errors='coerce').fillna(0)>0).sum())
        adequate=trade_days>=min_dni and share is not None and share<=max_udzial
        rows.append({'wariant':variant,'dni_kalendarzowe':len(g),'dni_z_transakcjami':trade_days,
                     'dni_dodatnie':int((v>0).sum()),'suma_R':round(total,4),
                     'najlepszy_dzien_R':round(best,4),'najgorszy_dzien_R':round(float(v.min()),4),
                     'udzial_najlepszego_dnia_w_zyskach_pct':round(100*share,2) if share is not None else None,
                     'status_jakosci':'WYSTARCZAJACA_DYSTRYBUCJA' if adequate else 'NIEWYSTARCZAJACE_DANE_LUB_KONCENTRACJA',
                     'uwaga':'Tylko diagnostyka OOS; nie jest testem istotnosci ani pozwoleniem na handel'})
    return pd.DataFrame(rows,columns=cols)


def elliott_ab_odpornosc(daily):
    """Leave-one-day-out na dniach zamkniecia, z porownaniem sparowanym.
    Osobno raportuje usuniecie najlepszego dnia wariantu oraz najlepszego
    dnia *roznicy* Elliott-BAZA (ta sama data usunieta w obu wariantach).
    """
    cols=['scenariusz','usuniety_dzien_utc','BAZA_R','ELLIOTT_R','przewaga_E_minus_B_R',
          'elliott_dodatni','elliott_lepszy_od_bazy','dni_z_transakcjami','uwaga']
    if daily is None or daily.empty or not {'wariant','dzien_utc','suma_R'}.issubset(daily.columns):
        return pd.DataFrame(columns=cols)
    x=daily.copy()
    x['suma_R']=pd.to_numeric(x['suma_R'],errors='coerce').fillna(0)
    x=x[x['wariant'].isin(['BAZA','ELLIOTT_HIPOTEZA'])]
    if x.empty: return pd.DataFrame(columns=cols)
    pv=x.pivot_table(index='dzien_utc',columns='wariant',values='suma_R',aggfunc='sum',fill_value=0)
    for name in ('BAZA','ELLIOTT_HIPOTEZA'):
        if name not in pv: pv[name]=0.0
    pv=pv.sort_index(); pv['delta']=pv['ELLIOTT_HIPOTEZA']-pv['BAZA']
    totals=pv[['BAZA','ELLIOTT_HIPOTEZA']].sum()
    days=int(x.groupby('dzien_utc')['transakcje'].sum().gt(0).sum()) if 'transakcje' in x else int(len(pv))
    scenarios=[('CALOSC',None,'Pelny badany okres')]
    for variant,short in [('BAZA','BAZA'),('ELLIOTT_HIPOTEZA','ELLIOTT')]:
        scenarios.append(('BEZ_NAJLEPSZEGO_DNIA_'+short,pv[variant].idxmax(),
                          'Dzien najlepszy dla danego wariantu; osobne scenariusze'))
    scenarios.append(('BEZ_NAJLEPSZEGO_DNIA_PRZEWAGI',pv['delta'].idxmax(),
                      'Ta sama data wykluczona z obu wariantow; kontrola koncentracji przewagi'))
    rows=[]
    for mode,drop,note in scenarios:
        a=pv.drop(index=drop) if drop is not None else pv
        b=float(a['BAZA'].sum()); e=float(a['ELLIOTT_HIPOTEZA'].sum())
        rows.append({'scenariusz':mode,'usuniety_dzien_utc':str(drop) if drop is not None else '',
                     'BAZA_R':round(b,4),'ELLIOTT_R':round(e,4),
                     'przewaga_E_minus_B_R':round(e-b,4),'elliott_dodatni':bool(e>0),
                     'elliott_lepszy_od_bazy':bool(e>b),
                     'dni_z_transakcjami':max(0,days-(1 if drop is not None else 0)),
                     'uwaga':note+'; diagnostyka, nie walidacja OOS'})
    return pd.DataFrame(rows,columns=cols)


def elliott_ab_ocena_kosztow(stress):
    """Wyodrebnia czy Elliott jest dodatni BEZWZGLEDNIE i lepszy WZGLEDNIE.
    Nie traktuje dodatniej przewagi nad stratna BAZA jako rentownosci.
    """
    cols=['miejsca_kierunek','dodatkowy_koszt_R','kolejnosc_remisow','BAZA_R',
          'ELLIOTT_R','przewaga_E_minus_B_R','elliott_dodatni','elliott_lepszy_od_bazy',
          'wniosek','uwaga']
    if stress is None or stress.empty or not {'wariant','suma_R','miejsca_kierunek','dodatkowy_koszt_R','kolejnosc_remisow'}.issubset(stress.columns):
        return pd.DataFrame(columns=cols)
    rows=[]
    key=['miejsca_kierunek','dodatkowy_koszt_R','kolejnosc_remisow']
    for k,g in stress.groupby(key,dropna=False,sort=True):
        by={str(r['wariant']):pd.to_numeric(r['suma_R'],errors='coerce') for _,r in g.iterrows()}
        b=by.get('BAZA');e=by.get('ELLIOTT_HIPOTEZA')
        if b is None or e is None or not np.isfinite(b) or not np.isfinite(e): continue
        b=float(b);e=float(e)
        verdict=('DODATNI_I_LEPSZY' if e>0 and e>b else
                 'DODATNI_ALE_NIE_LEPSZY' if e>0 else
                 'LEPSZY_ALE_NIERENTOWNY' if e>b else 'NIERENTOWNY_I_NIE_LEPSZY')
        rows.append(dict(zip(key,k),BAZA_R=round(b,4),ELLIOTT_R=round(e,4),
                    przewaga_E_minus_B_R=round(e-b,4),elliott_dodatni=bool(e>0),
                    elliott_lepszy_od_bazy=bool(e>b),wniosek=verdict,
                    uwaga='Koszt dodatkowy poza bazowym; symulacja badawcza, bez depozytu/live'))
    return pd.DataFrame(rows,columns=cols)


def elliott_ab_leave_one_day_out(daily):
    """Sparowany test usuniecia kazdego dnia zamkniecia. Nie jest backtestem OOS.

    Tylko dni z co najmniej jedna zamknieta transakcja sa interpretowane
    jako informacyjne. Dni bez transakcji moga wystepowac w raporcie dziennym.
    """
    detail_cols=['dzien_utc','BAZA_dzien_R','ELLIOTT_dzien_R','przewaga_dzien_R',
        'BAZA_bez_dnia_R','ELLIOTT_bez_dnia_R','przewaga_bez_dnia_R',
        'zmiana_przewagi_po_usunieciu_R','przewaga_bez_dnia_dodatnia',
        'elliott_bez_dnia_dodatni','transakcje_razem','ranking_wplywu','uwaga']
    summary_cols=['dni_kalendarzowe','dni_z_transakcjami','dni_przewagi_elliotta',
        'dni_przewagi_bazy','dni_remisow','BAZA_R','ELLIOTT_R','przewaga_R',
        'najwiekszy_wklad_dzien_R','najwiekszy_wklad_data',
        'najwiekszy_ujemny_wklad_dzien_R','najwiekszy_ujemny_wklad_data',
        'min_przewaga_po_usunieciu_R','max_przewaga_po_usunieciu_R',
        'dni_ktorych_usuniecie_odwraca_przewage','status','uwaga']
    empty=(pd.DataFrame(columns=detail_cols),pd.DataFrame(columns=summary_cols))
    if daily is None or daily.empty or not {'wariant','dzien_utc','suma_R','transakcje'}.issubset(daily.columns):
        return empty
    x=daily[daily['wariant'].isin(['BAZA','ELLIOTT_HIPOTEZA'])].copy()
    x['suma_R']=pd.to_numeric(x['suma_R'],errors='coerce').fillna(0)
    x['transakcje']=pd.to_numeric(x['transakcje'],errors='coerce').fillna(0)
    x['dzien_utc']=pd.to_datetime(x['dzien_utc'],utc=True,errors='coerce').dt.strftime('%Y-%m-%d')
    x=x[x['dzien_utc'].notna()]
    if x.empty: return empty
    pv=x.pivot_table(index='dzien_utc',columns='wariant',values='suma_R',aggfunc='sum',fill_value=0)
    cnt=x.groupby('dzien_utc')['transakcje'].sum().reindex(pv.index,fill_value=0)
    for name in ('BAZA','ELLIOTT_HIPOTEZA'):
        if name not in pv: pv[name]=0.0
    pv=pv.sort_index()
    pv['delta']=pv['ELLIOTT_HIPOTEZA']-pv['BAZA']
    base=float(pv['BAZA'].sum()); ell=float(pv['ELLIOTT_HIPOTEZA'].sum()); delta=ell-base
    ranks=pv['delta'].abs().rank(method='first',ascending=False).astype(int)
    rows=[]
    for day,r in pv.iterrows():
        b=float(r['BAZA']);e=float(r['ELLIOTT_HIPOTEZA']);d=e-b
        rest=delta-d
        rows.append(dict(dzien_utc=str(day),BAZA_dzien_R=round(b,4),
            ELLIOTT_dzien_R=round(e,4),przewaga_dzien_R=round(d,4),
            BAZA_bez_dnia_R=round(base-b,4),ELLIOTT_bez_dnia_R=round(ell-e,4),
            przewaga_bez_dnia_R=round(rest,4),
            zmiana_przewagi_po_usunieciu_R=round(-d,4),
            przewaga_bez_dnia_dodatnia=bool(rest>0),
            elliott_bez_dnia_dodatni=bool(ell-e>0),
            transakcje_razem=int(cnt.loc[day]),ranking_wplywu=int(ranks.loc[day]),
            uwaga='Daty zamkniecia, obie strategie bez tego samego dnia; analiza badawcza'))
    details=pd.DataFrame(rows,columns=detail_cols)
    active=pv[cnt>0]
    active_delta=active['delta'] if len(active) else pd.Series(dtype=float)
    flips=sum((delta>0 and delta-d<=0) or (delta<0 and delta-d>=0)
              for d in active_delta) if delta!=0 else 0
    if len(active):
        best_day=str(active_delta.idxmax());worst_day=str(active_delta.idxmin())
        best=float(active_delta.max());worst=float(active_delta.min())
    else: best_day=worst_day='';best=worst=0.0
    summ=pd.DataFrame([dict(dni_kalendarzowe=len(pv),dni_z_transakcjami=len(active),
        dni_przewagi_elliotta=int((active_delta>0).sum()),
        dni_przewagi_bazy=int((active_delta<0).sum()),
        dni_remisow=int((active_delta==0).sum()),
        BAZA_R=round(base,4),ELLIOTT_R=round(ell,4),przewaga_R=round(delta,4),
        najwiekszy_wklad_dzien_R=round(best,4),najwiekszy_wklad_data=best_day,
        najwiekszy_ujemny_wklad_dzien_R=round(worst,4),najwiekszy_ujemny_wklad_data=worst_day,
        min_przewaga_po_usunieciu_R=round(float((delta-active_delta).min()),4) if len(active) else None,
        max_przewaga_po_usunieciu_R=round(float((delta-active_delta).max()),4) if len(active) else None,
        dni_ktorych_usuniecie_odwraca_przewage=int(flips),
        status=('BRAK_TRANSAKCJI' if not len(active) else
                'WRAZLIWA_NA_POJEDYNCZY_DZIEN' if flips else
                'BRAK_ODWROCENIA_W_TESCIE_JEDNEGO_DNIA'),
        uwaga='Bez kosztow dodatkowych; przejscie testu nie dowodzi przewagi ani istotnosci statystycznej')],columns=summary_cols)
    return details,summ


def elliott_oos_postep(df, daily, start='2026-10-27T00:00:00Z'):
    """Liczniki OOS po rozliczonych okazjach, nie liczy duplikatow zdjec jako niezaleznych transakcji."""
    cols=['oos_od_utc','sygnaly_wszystkie','sygnaly_rozliczone','okazje_rozliczone',
          'zdjecia_rozliczone','dni_sygnalow','dni_z_zamknieciami',
          'sygnaly_z_faza','sygnaly_bez_fazy','status','uwaga']
    x=df.copy() if df is not None else pd.DataFrame()
    resolved=x[x['status'].isin(FINAL)].copy() if 'status' in x else x.iloc[0:0].copy()
    if len(resolved) and 'okazja_id' not in resolved: resolved=add_event_ids(resolved)
    snaps=pd.to_datetime(resolved['snapshot_utc'],utc=True,errors='coerce') if 'snapshot_utc' in resolved else pd.Series(dtype='datetime64[ns, UTC]')
    allsnaps=pd.to_datetime(x['snapshot_utc'],utc=True,errors='coerce') if 'snapshot_utc' in x else pd.Series(dtype='datetime64[ns, UTC]')
    g=elliott_oznaczenie(resolved)
    recognized=int((~g.isin(('BRAK','NIEOBLICZONE'))).sum())
    if daily is None or daily.empty: closed_days=0
    else:
        z=daily.copy()
        z['transakcje']=pd.to_numeric(z['transakcje'],errors='coerce').fillna(0)
        closed_days=int(z.loc[z['transakcje']>0,'dzien_utc'].nunique())
    row={'oos_od_utc':start,'sygnaly_wszystkie':len(x),'sygnaly_rozliczone':len(resolved),
         'okazje_rozliczone':int(resolved['okazja_id'].nunique()) if 'okazja_id' in resolved else 0,
         'zdjecia_rozliczone':int(snaps.nunique()),'dni_sygnalow':int(allsnaps.dt.floor('D').nunique()),
         'dni_z_zamknieciami':closed_days,'sygnaly_z_faza':recognized,
         'sygnaly_bez_fazy':len(resolved)-recognized,
         'status':'BRAK_DANYCH_OOS' if not len(resolved) else 'OBSERWACJA_OOS',
         'uwaga':'Rozliczone okazje i zdjecia nie sa w pelni niezalezne; wyłącznie diagnostyka'}
    return pd.DataFrame([row],columns=cols)


def elliott_ab_werdykt(daily, stress=None, min_dni=10, min_transakcji=30):
    """Konserwatywna etykieta naukowa; nigdy nie dopuszcza do handlu."""
    cols=['BAZA_R','ELLIOTT_R','przewaga_E_minus_B_R','dni_z_transakcjami',
          'transakcje_elliott','dodatni_po_koszcie_005R','stabilny_bez_jednego_dnia',
          'status','powody','uwaga']
    if daily is None or daily.empty: return pd.DataFrame([{
        'BAZA_R':None,'ELLIOTT_R':None,'przewaga_E_minus_B_R':None,
        'dni_z_transakcjami':0,'transakcje_elliott':0,'dodatni_po_koszcie_005R':False,
        'stabilny_bez_jednego_dnia':False,'status':'BRAK_POTWIERDZONEJ_PRZEWAGI',
        'powody':'brak rozliczonych obserwacji','uwaga':'Diagnostyka; bez wpływu na handel'}],columns=cols)
    x=daily.copy()
    x['suma_R']=pd.to_numeric(x['suma_R'],errors='coerce').fillna(0)
    x['transakcje']=pd.to_numeric(x['transakcje'],errors='coerce').fillna(0)
    totals=x.groupby('wariant')['suma_R'].sum()
    b=float(totals.get('BAZA',0)); e=float(totals.get('ELLIOTT_HIPOTEZA',0))
    n=int(x.loc[x['wariant']=='ELLIOTT_HIPOTEZA','transakcje'].sum())
    days=int(x.loc[x['transakcje']>0,'dzien_utc'].nunique())
    _, summary=elliott_ab_leave_one_day_out(daily)
    stable=bool(len(summary) and summary.iloc[0]['dni_ktorych_usuniecie_odwraca_przewage']==0 and e>b)
    # Dodatkowe 0.05 R poza bazowymi kosztami, bez estymacji nowego slippage.
    cost_ok=bool(e-0.05*n>0)
    reasons=[]
    if e<=0: reasons.append('ujemny_lub_zerowy_R')
    if e<=b: reasons.append('brak_przewagi_nad_baza')
    if days<min_dni: reasons.append('za_malo_dni')
    if n<min_transakcji: reasons.append('za_malo_transakcji')
    if not stable: reasons.append('wrazliwy_na_pominiecie_dnia')
    if not cost_ok: reasons.append('nieodporny_na_dodatkowy_koszt')
    # Nawet spelnienie warunkow diagnostycznych NIE jest potwierdzeniem w sensie handlowym.
    status='BRAK_POTWIERDZONEJ_PRZEWAGI' if reasons else 'KRYTERIA_BADAWCZE_SPELNIONE_BEZ_AUTORYZACJI'
    return pd.DataFrame([{'BAZA_R':round(b,4),'ELLIOTT_R':round(e,4),
        'przewaga_E_minus_B_R':round(e-b,4),'dni_z_transakcjami':days,
        'transakcje_elliott':n,'dodatni_po_koszcie_005R':cost_ok,
        'stabilny_bez_jednego_dnia':stable,'status':status,
        'powody':'; '.join(reasons),'uwaga':'Opisowa kontrola, nie test istotnosci; nigdy nie zmienia do_handlu'}],columns=cols)


def elliott_oos_niezaleznosc(df, cluster_hours=1):
    """Opisowa miara koncentracji obserwacji, NIE statystyczna niezaleznosc sygnalow."""
    cols=['sygnaly','rozliczone','okazje','zdjecia_utc','klastry_czasowe_1h',
          'max_sygnalow_na_zdjecie','max_okazji_na_klaster', 'status','uwaga']
    x=df.copy() if df is not None else pd.DataFrame()
    if x.empty or 'snapshot_utc' not in x:
        return pd.DataFrame([dict(zip(cols,[0,0,0,0,0,0,0,'BRAK_DANYCH_OOS',
          'Klastry godzinowe sa miara skupienia, nie dowodem niezaleznosci']))],columns=cols)
    x['czas']=pd.to_datetime(x['snapshot_utc'],utc=True,errors='coerce')
    x=x[x['czas'].notna()].copy()
    resolved=x[x['status'].isin(FINAL)].copy() if 'status' in x else x.iloc[0:0].copy()
    if len(resolved) and 'okazja_id' not in resolved:
        resolved=add_event_ids(resolved)
    photos=x.groupby('czas').size()
    if len(resolved):
        resolved['cluster']=resolved['czas'].dt.floor(f'{int(cluster_hours)}h')
        group_counts=resolved.groupby('cluster')['okazja_id'].nunique()
    else: group_counts=pd.Series(dtype=int)
    n_unique=int(resolved['okazja_id'].nunique()) if 'okazja_id' in resolved else 0
    clusters=int(len(group_counts))
    status='BRAK_ROZLICZEN' if not len(resolved) else ('SKUPIONE_OBSERWACJE' if clusters<10 or (len(group_counts) and group_counts.max()>1) else 'WIELE_KLASTROW_NIE_DOWOD_NIEZALEZNOSCI')
    return pd.DataFrame([dict(zip(cols,[len(x),len(resolved),n_unique,int(photos.size),clusters,
         int(photos.max()) if len(photos) else 0,int(group_counts.max()) if len(group_counts) else 0,
         status,'Klastry czasowe nie usuwaja wspolnego ryzyka BTC ani korelacji miedzy coinami']))],columns=cols)


def elliott_oos_fazy(df):
    """Wyniki faz z chwili sygnalu; bez retrospektywnego rozpoznawania fal."""
    cols=['segment','transakcje','okazje','zdjecia','suma_R','sredni_R','status','uwaga']
    x=df.copy() if df is not None else pd.DataFrame()
    if x.empty or 'status' not in x: return pd.DataFrame(columns=cols)
    x=x[x['status'].isin(FINAL)].copy()
    if not len(x): return pd.DataFrame(columns=cols)
    x['r']=pd.to_numeric(x['r'],errors='coerce')
    x=x[x['r'].notna() & np.isfinite(x['r'])].copy()
    if not len(x): return pd.DataFrame(columns=cols)
    if 'okazja_id' not in x: x=add_event_ids(x)
    x['segment']=np.where(elliott_oznaczenie(x).isin(('BRAK','NIEOBLICZONE')),
                         'BEZ_ROZPOZNANEJ_FAZY','ROZPOZNANA_FAZA')
    rows=[]
    for segment in ('ROZPOZNANA_FAZA','BEZ_ROZPOZNANEJ_FAZY'):
        z=x[x['segment']==segment]
        n=int(z['okazja_id'].nunique())
        rows.append({'segment':segment,'transakcje':len(z),'okazje':n,
          'zdjecia':int(z['snapshot_utc'].nunique()) if 'snapshot_utc' in z else 0,
          'suma_R':round(float(z['r'].sum()),4),'sredni_R':round(float(z['r'].mean()),4) if len(z) else None,
          'status':'ZA_MALA_PROBA' if n<20 else 'OBSERWACJA_BEZ_AUTORYZACJI',
          'uwaga':'Wyniki surowych rozliczonych sygnalow, NIE wyniki portfela A/B'})
    return pd.DataFrame(rows,columns=cols)


def elliott_oos_alarm(progress, phases, verdict):
    """Informacyjny alert nie ma sciezki do decyzji do_handlu."""
    n=int(progress.iloc[0]['okazje_rozliczone']) if progress is not None and len(progress) else 0
    recognized=phases.loc[phases.segment=='ROZPOZNANA_FAZA'] if phases is not None and len(phases) else pd.DataFrame()
    r=float(recognized.iloc[0]['sredni_R']) if len(recognized) and pd.notna(recognized.iloc[0]['sredni_R']) else None
    e=float(verdict.iloc[0]['ELLIOTT_R']) if verdict is not None and len(verdict) and pd.notna(verdict.iloc[0]['ELLIOTT_R']) else None
    alerts=[]
    if n<20: alerts.append('NIEWYSTARCZAJACA_PROBA_OOS')
    if e is not None and e<0: alerts.append('PORTFEL_ELLIOTT_OOS_UJEMNY')
    if r is not None and r<0: alerts.append('ROZPOZNANE_FAZY_OOS_UJEMNE')
    if not alerts: alerts.append('OBSERWACJA_BEZ_POTWIERDZENIA')
    return pd.DataFrame([{'status':'; '.join(alerts),'okazje_rozliczone':n,
       'elliott_portfel_R':e,'fazy_sredni_R':r,
       'uwaga':'Alert badawczy; nigdy nie zmienia uprawnien handlowych'}])


OOS_CHECKPOINT_V686 = 'elliott_oos_checkpoint_v686.json'


def elliott_oos_delta(df, previous=None):
    """Przyrost identyfikatorow pomiedzy uruchomieniami; niezaleznosc statystyczna NIE jest zakladana."""
    cols=['stan_porownania','sygnaly_obecnie','nowe_sygnaly','okazje_obecnie',
          'nowe_okazje','rozliczone_okazje_obecnie','nowe_rozliczone_okazje',
          'nowe_rozliczone_z_faza','uwaga']
    x=df.copy() if df is not None else pd.DataFrame()
    if len(x) and 'okazja_id' not in x: x=add_event_ids(x)
    # Unikatowy sygnal = event_id + snapshot. Ten sam event mierzony ponownie nie jest nowa okazja.
    ev=x['event_id'].fillna('').astype(str) if 'event_id' in x else pd.Series(['']*len(x),index=x.index)
    snap=x['snapshot_utc'].fillna('').astype(str) if 'snapshot_utc' in x else pd.Series(['']*len(x),index=x.index)
    sig=set((ev+'|'+snap).tolist())
    occ=set(x['okazja_id'].dropna().astype(str)) if 'okazja_id' in x else set()
    resolved=x[x['status'].isin(FINAL)].copy() if 'status' in x else x.iloc[0:0].copy()
    done=set(resolved['okazja_id'].dropna().astype(str)) if 'okazja_id' in resolved else set()
    recognized=resolved.loc[~elliott_oznaczenie(resolved).isin(('BRAK','NIEOBLICZONE'))]
    phases=set(recognized['okazja_id'].dropna().astype(str)) if 'okazja_id' in recognized else set()
    state={'schema':1,'sygnaly':sorted(sig),'okazje':sorted(occ),
           'rozliczone':sorted(done),'rozliczone_fazy':sorted(phases)}
    valid=isinstance(previous,dict) and previous.get('schema')==1 and all(isinstance(previous.get(k),list) for k in ('sygnaly','okazje','rozliczone','rozliczone_fazy'))
    old={k:set(previous[k]) for k in ('sygnaly','okazje','rozliczone','rozliczone_fazy')} if valid else None
    row={'stan_porownania':'POROWNANO' if valid else 'BRAK_POROWNANIA',
         'sygnaly_obecnie':len(sig),'nowe_sygnaly':len(sig-old['sygnaly']) if valid else None,
         'okazje_obecnie':len(occ),'nowe_okazje':len(occ-old['okazje']) if valid else None,
         'rozliczone_okazje_obecnie':len(done),
         'nowe_rozliczone_okazje':len(done-old['rozliczone']) if valid else None,
         'nowe_rozliczone_z_faza':len(phases-old['rozliczone_fazy']) if valid else None,
         'uwaga':'Roznica wobec poprzedniego zapisanego stanu, nie dowod niezaleznosci; brak stanu nie oznacza zera nowych sygnalow'}
    return pd.DataFrame([row],columns=cols),state


def elliott_oos_populacje(progress, phases, portfolio):
    """Rozne mianowniki: sygnaly i ich wyniki vs pozycje wybrane do portfela."""
    rows=[]
    p=progress.iloc[0] if progress is not None and len(progress) else {}
    phase_r=phases if phases is not None else pd.DataFrame()
    rows.append({'populacja':'WSZYSTKIE_SYGNALY','liczba_rozliczonych':int(p.get('sygnaly_rozliczone',0)),
                 'unikalne_okazje':int(p.get('okazje_rozliczone',0)),
                 'rozliczone_z_rozpoznana_faza':int(phase_r.loc[phase_r.segment=='ROZPOZNANA_FAZA','transakcje'].sum()) if len(phase_r) else 0,
                 'suma_R':float(phase_r['suma_R'].sum()) if len(phase_r) else None,
                 'uwaga':'Surowe rozliczone sygnaly; suma R nie jest wynikiem portfela'})
    for v in ('BAZA','ELLIOTT_HIPOTEZA'):
        z=portfolio[portfolio['wariant']==v] if portfolio is not None and 'wariant' in portfolio else pd.DataFrame()
        if len(z):
            n=int(pd.to_numeric(z['transakcje'],errors='coerce').fillna(0).iloc[0]) if 'transakcje' in z else 0
            r=float(pd.to_numeric(z['suma_R'],errors='coerce').fillna(0).iloc[0]) if 'suma_R' in z else None
        else: n=0; r=None
        rows.append({'populacja':'PORTFEL_'+v,'liczba_rozliczonych':n,'unikalne_okazje':None,
                     'rozliczone_z_rozpoznana_faza':None,'suma_R':r,
                     'uwaga':'Wybrane transakcje przy ograniczeniach portfela; nie sumowac z surowymi sygnalami'})
    return pd.DataFrame(rows)


def test_tradfi_bez_blokad():
    """V6.93: brak bramki krypto nie omija plynnosci, rynku ani budzetu ryzyka."""
    def row(**k):
        r={'symbol':'X','side':'SHORT','setup_type':'CONT_SWEEP','regime_1h':'range',
           'ocena':'D','filtr':'koszt;tradfi_bez_testu','tradable':True,
           'not_tradable_reason':'','status_bramki':'OBSERWACJA','asset_type':'Stocks',
           'trend_4h':0,'btc_trend_1h':1,'corr_btc':0.1,
           'ilosc_za_mala':False,'strata_sl_usdt':1.0}
        r.update(k); return r
    shock={'alarm':True}; protection={'aktywny':False,'strony':{}}
    a=row(); apply_conditions([a],shock,protection)
    assert a['do_handlu'] and status_setupu(a)=='BEZ BRAMKI' and 'ocena D' in a['ostrzezenia_tradfi']
    for reason in ('book/wolumen','TradFi zamkniety','cena niespojna','anomalia wolumenu',''):
        b=row(tradable=False,not_tradable_reason=reason)
        apply_conditions([b],shock,protection)
        assert not b['do_handlu'] and b['blokady_wykonania_tradfi'], reason
    c=row(ilosc_za_mala=True,ryzyko_min_usdt=3.0,strata_sl_usdt=1.0)
    apply_conditions([c],shock,protection)
    assert not c['do_handlu'] and 'minimum kontraktu' in c['warunki']
    d=row(ilosc_za_mala=True,ryzyko_min_usdt=None,strata_sl_usdt=1.0)
    apply_conditions([d],shock,protection)
    assert not d['do_handlu']
    e=row(asset_type='COIN',tradable=False,not_tradable_reason='book/wolumen')
    apply_conditions([e],shock,protection)
    assert not e['do_handlu'] and 'ostrzezenia_tradfi' not in e
    f=row(); apply_conditions([f],None,None,{'limit_osiagniety':True,'wynik_R':-3})
    assert not f['do_handlu'] and f['warunki'].startswith('dzienny limit')
    g=row(asset_type='Metals'); apply_conditions([g],None,None)
    assert g['do_handlu']
    return 'OK (twarde wykonanie/ryzyko/limit; statystyka TradFi informacyjna; krypto bez zmian)'


def test_tradfi_24h():
    """V6.88: martwe swiece = zamkniety rynek; zywe swiece poza sesja USA = handel; podzial sesji w tradfi_table."""
    a=np.zeros((10,8)); a[:,H]=2.0; a[:,L]=1.0; a[:,V]=5.0
    assert not swiece_martwe(a)
    b=a.copy(); b[-6:,V]=0.0; assert swiece_martwe(b)
    c=a.copy(); c[-6:,H]=1.0; assert swiece_martwe(c)
    assert TRADFI_24H and STOCK_TYPES <= TRADFI_TYPES
    # 2026-10-27 15:00 UTC = 11:00 NY (sesja), 2026-10-27 09:00 UTC = 05:00 NY (poza sesja)
    d=pd.DataFrame([{'typ_wersja':'CONT_SWEEP.v4','side':'SHORT','status':'TP1','r':1.0,'edge_R':0.5,'filtr':'',
                     'snapshot_utc':'2026-10-27T15:00','okazja_id':'a','event_id':'a','control_R':0.5,'filled':True},
                    {'typ_wersja':'CONT_SWEEP.v4','side':'SHORT','status':'SL','r':-1.0,'edge_R':-0.5,'filtr':'',
                     'snapshot_utc':'2026-10-27T09:00','okazja_id':'b','event_id':'b','control_R':-0.5,'filled':True}])
    t=tradfi_table(d,0).iloc[0]
    assert t['transakcje_sesja_USA']==1 and t['transakcje_poza_sesja']==1
    assert t['sredni_R_sesja_USA']==1.0 and t['sredni_R_poza_sesja']==-1.0
    return 'OK (martwe swiece, 24h, podzial sesji)'


def test_market_ab():
    """V6.87: 1 slot, na tym samym zdjeciu LIMIT z wyzszym RR (nie wypelni sie) i MARKET (TP1).
    BAZA wybiera LIMIT -> 0 transakcji; MARKET_PIERWSZE wybiera MARKET -> 1 transakcja. Domyslne A/B bez zmian."""
    base={'snapshot_utc':'2026-10-01T10:00:00Z','side':'LONG','setup_type':'CONT_SWEEP','typ_wersja':'CONT_SWEEP.v4',
          'elliott_vs_setup':'BRAK','filtr':''}
    lim=dict(base,symbol='LIM',entry_type='LIMIT',entry=1,zone_ref=1,status='niewypelniony',r=0.0,rr_tp1_net=3.0,
             wait_h=3,fill_utc=None,exit_utc=None,event_id='lim')
    mkt=dict(base,symbol='MKT',typ_wersja='CONT_SWEEP.v4.M',entry_type='MARKET',entry=2,zone_ref=2,status='TP1',r=1.5,
             rr_tp1_net=1.8,wait_h=0,fill_utc='2026-10-01T10:01:00Z',exit_utc='2026-10-01T12:00:00Z',event_id='mkt')
    d=pd.DataFrame([lim,mkt])
    ab=elliott_portfel_ab(d,miejsca=1,warianty=('BAZA','MARKET_PIERWSZE')).set_index('wariant')
    assert ab.loc['BAZA','transakcje']==0 and ab.loc['BAZA','niewypelnione']==1
    assert ab.loc['MARKET_PIERWSZE','transakcje']==1 and abs(ab.loc['MARKET_PIERWSZE','suma_R']-1.5)<1e-9
    assert list(elliott_portfel_ab(d,miejsca=1)['wariant'])==['BAZA','ELLIOTT_HIPOTEZA']
    rep=market_ab_raport(d,miejsca=1)
    c=rep[(rep.populacja=='FILTRY_Z_CHWILI_SYGNALU')&(rep.okres=='CALOSC')&(rep.kolejnosc_remisow=='normalna')].set_index('wariant')
    assert abs(c.loc['MARKET_PIERWSZE','przewaga_vs_BAZA_R']-1.5)<1e-9
    # CONT_SWEEP LONG LIMIT jest dzis wylaczony -> w AKTUALNE_REGULY zostaje tylko MARKET (obie wersje go biora)
    a=rep[(rep.populacja=='AKTUALNE_REGULY')&(rep.okres=='CALOSC')&(rep.kolejnosc_remisow=='normalna')].set_index('wariant')
    assert a.loc['BAZA','transakcje']==1 and a.loc['MARKET_PIERWSZE','transakcje']==1
    return 'OK (LIMIT blokuje BAZE, MARKET pierwszy, domyslne A/B, populacje)'


def test_elliott_v686():
    d=pd.DataFrame([{'event_id':'s1','okazja_id':'o1','snapshot_utc':'2026-10-27T08:00:00Z',
                     'status':next(iter(FINAL)),'elliott_vs_setup':'PO_IMPULSIE','r':1.0}])
    a,old=elliott_oos_delta(d)
    assert a.iloc[0]['stan_porownania']=='BRAK_POROWNANIA' and pd.isna(a.iloc[0]['nowe_okazje'])
    b,_=elliott_oos_delta(d,old)
    assert b.iloc[0]['nowe_sygnaly']==0 and b.iloc[0]['nowe_rozliczone_okazje']==0
    d2=pd.concat([d,pd.DataFrame([{'event_id':'s2','okazja_id':'o2','snapshot_utc':'2026-10-27T09:00:00Z',
                                   'status':next(iter(FINAL)),'elliott_vs_setup':'BRAK','r':-1.0}])],ignore_index=True)
    c,_=elliott_oos_delta(d2,old)
    assert c.iloc[0]['nowe_sygnaly']==1 and c.iloc[0]['nowe_okazje']==1 and c.iloc[0]['nowe_rozliczone_z_faza']==0
    pop=elliott_oos_populacje(elliott_oos_postep(d2,pd.DataFrame()),elliott_oos_fazy(d2),pd.DataFrame([
        {'wariant':'BAZA','transakcje':1,'suma_R':-1}, {'wariant':'ELLIOTT_HIPOTEZA','transakcje':1,'suma_R':1}]))
    assert len(pop)==3 and pop.iloc[0]['liczba_rozliczonych']==2 and pop.iloc[1]['liczba_rozliczonych']==1
    return 'OK (pierwszy przebieg, brak duplikatow, przyrost i populacje)'


def test_elliott_v685():
    assert elliott_oos_niezaleznosc(pd.DataFrame()).iloc[0]['status']=='BRAK_DANYCH_OOS'
    data=pd.DataFrame([{'snapshot_utc':'2026-10-27T01:00:00Z','status':'tp','r':1.,
      'okazja_id':'a','elliott_vs_setup':'PO_IMPULSIE'},
      {'snapshot_utc':'2026-10-27T01:05:00Z','status':'sl','r':-1.,
      'okazja_id':'b','elliott_vs_setup':'BRAK'}])
    # FINAL sprawdzamy zgodnie z rzeczywista lista statusow kolektora.
    data.loc[0,'status']=next(iter(FINAL))
    data.loc[1,'status']=next(iter(FINAL))
    indep=elliott_oos_niezaleznosc(data)
    assert indep.iloc[0]['klastry_czasowe_1h']==1 and indep.iloc[0]['okazje']==2
    phases=elliott_oos_fazy(data)
    assert set(phases['segment'])=={'ROZPOZNANA_FAZA','BEZ_ROZPOZNANEJ_FAZY'}
    pr=elliott_oos_postep(data,pd.DataFrame())
    al=elliott_oos_alarm(pr,phases,elliott_ab_werdykt(pd.DataFrame()))
    assert 'NIEWYSTARCZAJACA_PROBA_OOS' in al.iloc[0]['status']
    return 'OK (koncentracja godzinowa, fazy, pusta proba, alert)'


def test_elliott_v684():
    empty=elliott_ab_werdykt(pd.DataFrame())
    assert empty.iloc[0]['status']=='BRAK_POTWIERDZONEJ_PRZEWAGI'
    d=pd.DataFrame([{'wariant':v,'dzien_utc':day,'suma_R':r,'transakcje':1}
        for v, arr in [('BAZA',[-1.,-1.]),('ELLIOTT_HIPOTEZA',[-0.2,-0.2])]
        for day,r in zip(('2026-10-27','2026-10-28'),arr)])
    v=elliott_ab_werdykt(d)
    assert v.iloc[0]['status']=='BRAK_POTWIERDZONEJ_PRZEWAGI'
    assert 'ujemny_lub_zerowy_R' in v.iloc[0]['powody']
    prog=elliott_oos_postep(pd.DataFrame(),pd.DataFrame())
    assert prog.iloc[0]['okazje_rozliczone']==0
    assert prog.iloc[0]['status']=='BRAK_DANYCH_OOS'
    return 'OK (werdykt bez automatycznej zgody; puste OOS; ujemny R)'


def test_elliott_v683():
    days=pd.DataFrame([{'wariant':v,'dzien_utc':d,'suma_R':r,'transakcje':int(r!=0)}
      for v,vals in [('BAZA',[-2.,0.,1.,0.]),('ELLIOTT_HIPOTEZA',[2.,0.,-1.,0.])]
      for d,r in zip(['2026-10-27','2026-10-28','2026-10-29','2026-10-30'],vals)])
    a,b=elliott_ab_leave_one_day_out(days)
    assert len(a)==4 and len(b)==1
    assert b.iloc[0]['przewaga_R']==2.0
    assert b.iloc[0]['dni_z_transakcjami']==2
    assert b.iloc[0]['dni_ktorych_usuniecie_odwraca_przewage']==1
    assert b.iloc[0]['status']=='WRAZLIWA_NA_POJEDYNCZY_DZIEN'
    assert a.loc[a.dzien_utc=='2026-10-27','przewaga_bez_dnia_R'].iloc[0]==-2
    assert a.loc[a.dzien_utc=='2026-10-28','transakcje_razem'].iloc[0]==0
    assert elliott_ab_leave_one_day_out(pd.DataFrame())[0].empty
    return 'OK (wszystkie daty, remis, obrot przewagi, puste OOS)'


def test_elliott_v682():
    days=pd.DataFrame([{'wariant':v,'dzien_utc':d,'suma_R':r,'transakcje':1}
      for v,vals in [('BAZA',[-1.,2.,-1.]),('ELLIOTT_HIPOTEZA',[0.,4.,-2.])]
      for d,r in zip(['2026-10-27','2026-10-28','2026-10-29'],vals)])
    z=elliott_ab_odpornosc(days)
    assert len(z)==4
    assert z.loc[z.scenariusz=='CALOSC','przewaga_E_minus_B_R'].iloc[0]==2
    pair=z[z.scenariusz=='BEZ_NAJLEPSZEGO_DNIA_PRZEWAGI'].iloc[0]
    assert pair['usuniety_dzien_utc']=='2026-10-28' and pair['ELLIOTT_R']==-2 and pair['BAZA_R']==-2
    st=pd.DataFrame([{'wariant':v,'suma_R':r,'miejsca_kierunek':3,
                      'dodatkowy_koszt_R':0.05,'kolejnosc_remisow':'normalna'}
                     for v,r in [('BAZA',-5.),('ELLIOTT_HIPOTEZA',-2.)]])
    q=elliott_ab_ocena_kosztow(st).iloc[0]
    assert q['wniosek']=='LEPSZY_ALE_NIERENTOWNY' and not q['elliott_dodatni']
    assert elliott_ab_odpornosc(pd.DataFrame()).empty
    return 'OK (dzien najlepszego wyniku, przewaga sparowana, koszt bezwzgledny)'


def test_elliott_v681():
    d=pd.DataFrame([
      {'snapshot_utc':'2026-10-27T10:00:00Z','fill_utc':'2026-10-27T10:59:00Z',
       'exit_utc':'2026-10-28T11:00:00Z','symbol':'AAA','side':'LONG','status':'TP1',
       'r':1.0,'entry_type':'LIMIT','wait_h':1,'event_id':'a','score_R':10},
      {'snapshot_utc':'2026-10-27T11:10:00Z','fill_utc':'2026-10-27T11:15:00Z',
       'exit_utc':'2026-10-27T12:00:00Z','symbol':'BBB','side':'LONG','status':'TP1',
       'r':1.0,'entry_type':'MARKET','event_id':'b','score_R':1},
      {'snapshot_utc':'2026-10-29T10:00:00Z','fill_utc':'2026-10-29T11:01:00Z',
       'exit_utc':'2026-10-30T10:00:00Z','symbol':'CCC','side':'SHORT','status':'TP1',
       'r':1.0,'entry_type':'LIMIT','wait_h':1,'event_id':'c','score_R':1}])
    base=elliott_portfel_ab(d,miejsca=1).query("wariant=='BAZA'").iloc[0]
    assert base['zlecenia_wybrane']==2 and base['transakcje']==1,base.to_dict()
    daily=elliott_ab_dzienny(d,miejsca=1)
    assert daily.query("wariant=='BAZA' and dzien_utc=='2026-10-28'")['suma_R'].iloc[0]==1
    assert daily.query("wariant=='BAZA' and dzien_utc=='2026-10-27'")['suma_R'].iloc[0]==0
    q=elliott_oos_dzienna_jakosc(daily)
    assert q['status_jakosci'].eq('NIEWYSTARCZAJACE_DANE_LUB_KONCENTRACJA').all()
    return 'OK (chronologia, TTL, dzienne OOS)'


def test_elliott_raporty():
    d=pd.DataFrame([
        {'snapshot_utc':'2026-10-01T10:00:00Z','fill_utc':'2026-10-01T10:05:00Z',
         'exit_utc':'2026-10-01T11:00:00Z','symbol':'AAA','side':'LONG','setup_type':'ODBICIE',
         'typ_wersja':'ODBICIE.v2.M','entry_type':'MARKET','entry':10,'zone_ref':10,
         'status':'TP1','r':0.5,'elliott_vs_setup':'BRAK', 'score_R':1.0},
        {'snapshot_utc':'2026-10-01T10:00:00Z','fill_utc':'2026-10-01T10:05:00Z',
         'exit_utc':'2026-10-01T11:00:00Z','symbol':'BBB','side':'LONG','setup_type':'ODBICIE',
         'typ_wersja':'ODBICIE.v2.M','entry_type':'MARKET','entry':20,'zone_ref':20,
         'status':'SL','r':-1.0,'score_R':0.5},
    ])
    assert list(elliott_oznaczenie(d))==['BRAK','NIEOBLICZONE']
    report=elliott_statystyki(d)
    assert set(report['grupa'])=={'BRAK','NIEOBLICZONE'}
    ab=elliott_portfel_ab(d,miejsca=1)
    assert len(ab)==2 and (ab['transakcje']==1).all()
    assert (ab['maks_aktywne_LONG']<=1).all()
    # Przypadek regresyjny: wyzszy ranking LIMIT nie wypelnia sie.
    # Bez patrzenia w przyszlosc nie mozna wybrac zamiast niego slabszego MARKET.
    q=d.iloc[:1].copy()
    q.loc[q.index[0],'symbol']='LIM'
    q.loc[q.index[0],'entry_type']='LIMIT'
    q.loc[q.index[0],'status']='niewypelniony'
    q.loc[q.index[0],'r']=0.0
    q.loc[q.index[0],'score_R']=100.0
    q.loc[q.index[0],'fill_utc']=None
    q.loc[q.index[0],'exit_utc']='2026-10-01T11:00:00Z'
    q.loc[q.index[0],'event_id']='new_lim'
    m=d.iloc[:1].copy()
    m.loc[m.index[0],'symbol']='MKT'
    m.loc[m.index[0],'score_R']=1.0
    m.loc[m.index[0],'event_id']='new_mkt'
    test_ab=elliott_portfel_ab(pd.concat([q,m],ignore_index=True),miejsca=1)
    base=test_ab[test_ab['wariant']=='BAZA'].iloc[0]
    assert base['zlecenia_wybrane']==1 and base['transakcje']==0 and base['niewypelnione']==1
    assert base['suma_R']==0 and base['maks_aktywne_LONG']==1
    # V6.77 (poprawka): otwarta pozycja trzyma slot do konca danych, nie do TTL.
    o=d.iloc[:1].copy()
    o.loc[o.index[0],'symbol']='OPN'; o.loc[o.index[0],'status']='otwarty'; o.loc[o.index[0],'r']=None
    o.loc[o.index[0],'score_R']=100.0; o.loc[o.index[0],'event_id']='open1'; o.loc[o.index[0],'exit_utc']=None
    p2=d.iloc[:1].copy()
    p2.loc[p2.index[0],'symbol']='LATE'; p2.loc[p2.index[0],'event_id']='late1'
    p2.loc[p2.index[0],'snapshot_utc']='2026-10-05T10:00:00Z'
    p2.loc[p2.index[0],'fill_utc']='2026-10-05T10:05:00Z'; p2.loc[p2.index[0],'exit_utc']='2026-10-05T11:00:00Z'
    t2=elliott_portfel_ab(pd.concat([o,p2],ignore_index=True),miejsca=1)
    b2=t2[t2['wariant']=='BAZA'].iloc[0]
    assert b2['w_toku']==1 and b2['transakcje']==0 and b2['zlecenia_wybrane']==1
    # V6.78: LIMIT oczekujacy zwalnia slot po TTL, w przeciwienstwie do otwartej pozycji.
    pending=o.copy()
    pending.loc[pending.index[0],'symbol']='WAIT'
    pending.loc[pending.index[0],'status']='oczekuje'
    pending.loc[pending.index[0],'entry_type']='LIMIT'
    pending.loc[pending.index[0],'wait_h']=1.0
    pending.loc[pending.index[0],'event_id']='pending1'
    pending.loc[pending.index[0],'fill_utc']=None
    expiring=elliott_portfel_ab(pd.concat([pending,p2],ignore_index=True),miejsca=1)
    ep=expiring[expiring['wariant']=='BAZA'].iloc[0]
    assert ep['zlecenia_wybrane']==2 and ep['transakcje']==1 and ep['w_toku']==1
    _, weekly, exit_weekly=elliott_portfel_ab(d,return_weekly=True,return_exit_weekly=True)
    assert set(exit_weekly['wariant'])=={'BAZA','ELLIOTT_HIPOTEZA'}
    assert exit_weekly['tydzien_zamkniecia_utc'].eq('2026-09-28').all()
    assert elliott_oos_jakosc(d)['status_proby'].eq('NIEWYSTARCZAJACA_PROBA').all()
    assert elliott_oos_jakosc(d,min_okazje=1,min_zdjec=1)['status_proby'].eq('MINIMUM_ILOŚCIOWE').all()
    stress=elliott_portfel_ab(d,miejsca=1,extra_cost_R=0.10)
    assert (stress['max_drawdown_R']>=0).all()
    assert (elliott_portfel_ab(d.iloc[1:2],miejsca=1)['max_drawdown_R']>=1).all()
    assert (elliott_portfel_ab(d,miejsca=1,tie_reverse=True)['maks_aktywne_LONG']<=1).all()
    assert (stress['zlecenia_wybrane']==ab['zlecenia_wybrane']).all()
    assert all(abs(float(v)-float(b)+0.10)<0.0001 for v,b in zip(stress['suma_R'],ab['suma_R']))
    # Zwolnienie slotu po LIMIT wypelnionym tuz przed TTL: liczy sie zamkniecie pozycji.
    lim=d.iloc[:1].copy(); lim.loc[lim.index[0],'symbol']='FILL_LIM'
    lim.loc[lim.index[0],'entry_type']='LIMIT'; lim.loc[lim.index[0],'wait_h']=1.0
    lim.loc[lim.index[0],'fill_utc']='2026-10-01T10:59:00Z'; lim.loc[lim.index[0],'exit_utc']='2026-10-05T10:00:00Z'
    lim.loc[lim.index[0],'event_id']='filled_before_ttl'; lim.loc[lim.index[0],'score_R']=100
    later=d.iloc[:1].copy(); later.loc[later.index[0],'symbol']='LATER'
    later.loc[later.index[0],'snapshot_utc']='2026-10-02T10:00:00Z'
    later.loc[later.index[0],'fill_utc']='2026-10-02T10:05:00Z'
    later.loc[later.index[0],'exit_utc']='2026-10-02T11:00:00Z'
    later.loc[later.index[0],'event_id']='later'
    res=elliott_portfel_ab(pd.concat([lim,later],ignore_index=True),miejsca=1)
    assert (res['zlecenia_wybrane']==1).all() and (res['transakcje']==1).all()
    assert set(weekly['wariant'])=={'BAZA','ELLIOTT_HIPOTEZA'} and 'tydzien_utc' in weekly
    f = pd.DataFrame({'filtr': ['', None, 'koszt', 'typ_wylaczony']})               # V6.76 (poprawka)
    assert len(elliott_tylko_po_filtrach(f)) == 2
    return 'OK (BRAK/NIEOBLICZONE, statystyki, A/B bez lookahead fill, slot LIMIT, filtr)'

# V6.137 - research-only linear sensitivity of settled Elliott OOS trades.
# This is not a historical replay. It does not alter fills, exits, selection or risk gates.
# V6.151 - porownanie scenariuszy na identycznych zdarzeniach, tylko diagnostyka.
def cost_comparison_v151(df):
    cols=['event_id','snapshot_utc','symbol','typ_wersja','side','entry_type','status',
          'slip_bps_model','slip_bps_stale30','risk_pct','r_model','r_stale30_szacunek',
          'delta_R_model_minus_30','uwaga_metoda']
    if df is None or df.empty:
        empty=pd.DataFrame(columns=cols)
        return empty,pd.DataFrame(columns=['typ_wersja','side','entry_type','transakcje','sredni_R_model','sredni_R_stale30_szacunek','zmiana_sredniego_R','metoda'])
    d=df.copy()
    # Tylko rozliczone transakcje: nie przypisujemy wyniku oczekujacym i niewypelnionym.
    status=d['status'].astype(str) if 'status' in d else pd.Series('',index=d.index)
    final=status.isin(('SL','TP1','BE','TRAIL','timeout'))
    r=pd.to_numeric(d.get('r',pd.Series(index=d.index,dtype=float)),errors='coerce')
    slip=pd.to_numeric(d.get('slip_bps',pd.Series(index=d.index,dtype=float)),errors='coerce')
    entry=pd.to_numeric(d.get('entry',pd.Series(index=d.index,dtype=float)),errors='coerce')
    stop=pd.to_numeric(d.get('sl',pd.Series(index=d.index,dtype=float)),errors='coerce')
    # Przy MARKET faktyczna cena fill moze roznic sie od sygnalowego entry.
    # Z tego powodu wynik nie jest dokladnym replayem (faktyczny risk moze sie roznic).
    riskpct=100*(entry-stop).abs()/entry.replace(0,np.nan)
    valid=final & r.notna() & slip.notna() & riskpct.gt(0) & np.isfinite(riskpct)
    data=d.loc[valid].copy()
    if data.empty:
        empty=pd.DataFrame(columns=cols)
        return empty,pd.DataFrame(columns=['typ_wersja','side','entry_type','transakcje','sredni_R_model','sredni_R_stale30_szacunek','zmiana_sredniego_R','metoda'])
    ix=data.index
    # Dla TP1: tylko wejscie MARKET ponosi poslizg. Dla pozostalych wyjsc
    # dodatkowy poslizg na wyjsciu (taker); czesciowe wyjscie 1R nie jest
    # modelowane dokladnie, dlatego jawnie oznaczamy przyblizenie.
    market=data['entry_type'].astype(str).eq('MARKET')
    exit_taker=~data['status'].astype(str).eq('TP1')
    weight=market.astype(float)+exit_taker.astype(float)
    delta=(30.0-slip.loc[ix])*weight/(riskpct.loc[ix]*100.)
    data['slip_bps_model']=slip.loc[ix]
    data['slip_bps_stale30']=30.0
    data['risk_pct']=riskpct.loc[ix]
    data['r_model']=r.loc[ix]
    data['r_stale30_szacunek']=r.loc[ix]-delta
    data['delta_R_model_minus_30']=delta
    data['uwaga_metoda']='APROKSYMACJA_KOSZTOW_BEZ_REPLAY_FILL_EXIT'
    for k in cols:
        if k not in data: data[k]=None
    det=data[cols].copy()
    summary=(det.groupby(['typ_wersja','side','entry_type'],dropna=False)
        .agg(transakcje=('r_model','size'),sredni_R_model=('r_model','mean'),
             sredni_R_stale30_szacunek=('r_stale30_szacunek','mean'),
             zmiana_sredniego_R=('delta_R_model_minus_30','mean')).reset_index())
    summary['metoda']='POROWNANIE_KOSZTOW_NIE_PRZELICZA_BRAMKI'
    return det,summary


def cost_legs_v152(detail):
    """Rozpiska *dodatkowego* slip 30bps vs model, nie pelny rachunek prowizji.
    Nieznane faktyczne fill, koszty czesci +1R oraz ceny wyjsc sa jawnie oznaczone.
    """
    cols=['event_id','symbol','snapshot_utc','typ_wersja','side','entry_type','status',
          'model_bps','porownanie_bps','risk_pct_z_sygnalu','wejscie_taker_delta_R',
          'wyjscie_taker_delta_R','czesciowe_1R_delta_R','delta_R_suma_szacunek',
          'r_model','r_stale30_szacunek','weryfikowalnosc','uwaga']
    if detail is None or detail.empty:return pd.DataFrame(columns=cols)
    x=detail.copy()
    out=[]
    for _,r in x.iterrows():
        den=float(r['risk_pct'])*100.
        diff=float(r['slip_bps_stale30'])-float(r['slip_bps_model'])
        ent=diff/den if str(r['entry_type'])=='MARKET' else 0.
        ex=diff/den if str(r['status'])!='TP1' else 0.
        partial=None # brak dokladnego rozliczenia czesciowego 1R w archiwum
        est=ent+ex
        out.append(dict(event_id=r.get('event_id'),symbol=r.get('symbol'),snapshot_utc=r.get('snapshot_utc'),
             typ_wersja=r.get('typ_wersja'),side=r.get('side'),entry_type=r.get('entry_type'),status=r.get('status'),
             model_bps=r['slip_bps_model'],porownanie_bps=30.,risk_pct_z_sygnalu=r['risk_pct'],
             wejscie_taker_delta_R=ent,wyjscie_taker_delta_R=ex,czesciowe_1R_delta_R=partial,
             delta_R_suma_szacunek=est,r_model=r['r_model'],r_stale30_szacunek=r['r_stale30_szacunek'],
             weryfikowalnosc='APROKSYMACJA_BEZ_FILL_I_CZESCIOWEGO_1R',
             uwaga='Nie przelicza prowizji maker/taker; TP1 LIMIT: 0 bps w przyblizeniu; inne wyjscia: taker.'))
    return pd.DataFrame(out,columns=cols)


def gate_cost_diagnostic_v152(arch,detail,gate_now):
    """Warunkowy stres sredniego R; nie jest symulacja gate_table (t/edge, snapshot weights)."""
    cols=['typ_wersja','side','status_bramki_obecny','transakcje_ocenialne',
          'sredni_R_aktualny_ocenialne','sredni_R_30bps_szacunek','prog_min_sredni_R',
          'przekracza_prog_aktualny','przekracza_prog_30bps',
          'hipotetyczna_zmiana_wzgledem_progu','uwaga']
    if detail is None or detail.empty:return pd.DataFrame(columns=cols)
    gates={}
    if gate_now is not None and not gate_now.empty:
        for _,g in gate_now.iterrows():gates[(str(g.get('typ')),str(g.get('side')))]=str(g.get('status','BRAK'))
    rows=[]
    for (tv,side),x in detail.groupby(['typ_wersja','side'],dropna=False):
        a=float(x['r_model'].mean());b=float(x['r_stale30_szacunek'].mean());t=float(GATE_MIN_AVG_R)
        old=a>=t;new=b>=t
        rows.append(dict(typ_wersja=tv,side=side,status_bramki_obecny=gates.get((str(tv),str(side)),'BRAK'),
            transakcje_ocenialne=len(x),sredni_R_aktualny_ocenialne=a,sredni_R_30bps_szacunek=b,
            prog_min_sredni_R=t,przekracza_prog_aktualny=old,przekracza_prog_30bps=new,
            hipotetyczna_zmiana_wzgledem_progu=bool(old!=new),
            uwaga='TYLKO PROG SREDNIEGO R; bez daty regul, filtrow bramki, wag zdjec, t_edge, edge i powtornej symulacji; NIE ZMIENIA BRAMKI'))
    return pd.DataFrame(rows,columns=cols)


def test_cost_legs_v152():
    d=pd.DataFrame([dict(event_id='a',symbol='A',snapshot_utc='x',typ_wersja='T',side='LONG',entry_type='MARKET',
             status='SL',slip_bps_model=10.,slip_bps_stale30=30.,risk_pct=1.,r_model=-1.,r_stale30_szacunek=-1.4),
             dict(event_id='b',symbol='B',snapshot_utc='x',typ_wersja='T',side='LONG',entry_type='LIMIT',
             status='TP1',slip_bps_model=10.,slip_bps_stale30=30.,risk_pct=1.,r_model=2.,r_stale30_szacunek=2.)])
    l=cost_legs_v152(d)
    assert len(l)==2 and abs(float(l.iloc[0]['delta_R_suma_szacunek'])-.4)<1e-10
    assert float(l.iloc[1]['delta_R_suma_szacunek'])==0.
    assert l['czesciowe_1R_delta_R'].isna().all()
    g=gate_cost_diagnostic_v152(None,d,pd.DataFrame())
    assert len(g)==1 and g.iloc[0]['transakcje_ocenialne']==2
    assert 'NIE ZMIENIA BRAMKI' in g.iloc[0]['uwaga']
    return 'PASS (nogi kosztowe, LIMIT TP1, czesc +1R nieznana, diagnostyka progu)'


def test_cost_comparison_v151():
    sample=pd.DataFrame([
       {'status':'SL','r':-1.,'slip_bps':10.,'entry':100.,'sl':99.,'entry_type':'MARKET','typ_wersja':'T','side':'LONG'},
       {'status':'TP1','r':2.,'slip_bps':10.,'entry':100.,'sl':99.,'entry_type':'LIMIT','typ_wersja':'T','side':'LONG'},
       {'status':'oczekuje','r':0.,'slip_bps':10.,'entry':100.,'sl':99.,'entry_type':'LIMIT','typ_wersja':'T','side':'LONG'},
    ])
    detail,stats=cost_comparison_v151(sample)
    assert len(detail)==2 and len(stats)==2
    assert abs(float(detail.iloc[0]['delta_R_model_minus_30'])-0.4)<1e-9
    assert abs(float(detail.iloc[1]['delta_R_model_minus_30'])-0.0)<1e-9
    return 'PASS (market SL, limit TP, pomijanie nierozliczonych)'

def sensitivity_oos_v137(trades, bps_levels=(10.0, 20.0, 30.0)):
    cols=['wariant','scenariusz_slip_bps','transakcje','ocenialne','nieocenialne',
          'suma_R_przyblizona','sredni_R_przyblizony','suma_R_baza',
          'metoda','ograniczenie']
    if trades is None or trades.empty:
        return pd.DataFrame(columns=cols)
    out=[]
    for variant,g in trades.groupby('wariant',dropna=False):
        r=pd.to_numeric(g.get('r_netto',pd.Series(index=g.index,dtype=float)),errors='coerce')
        slip=pd.to_numeric(g.get('slip_bps',pd.Series(index=g.index,dtype=float)),errors='coerce')
        risk=pd.to_numeric(g.get('risk_pct',pd.Series(index=g.index,dtype=float)),errors='coerce')
        valid=r.notna() & slip.notna() & risk.gt(0) & np.isfinite(r) & np.isfinite(slip) & np.isfinite(risk)
        for bps in bps_levels:
            # risk_pct is a percentage: 2.5% = 250 basis points of entry.
            # One-way approximation: shift in cost bps divided by risk distance bps.
            sim=r.loc[valid]+(slip.loc[valid]-float(bps))/(risk.loc[valid]*100.0)
            out.append({'wariant':str(variant),'scenariusz_slip_bps':float(bps),
                        'transakcje':int(len(g)),'ocenialne':int(valid.sum()),
                        'nieocenialne':int((~valid).sum()),
                        'suma_R_przyblizona':round(float(sim.sum()),5) if len(sim) else None,
                        'sredni_R_przyblizony':round(float(sim.mean()),6) if len(sim) else None,
                        'suma_R_baza':round(float(r.loc[valid].sum()),5) if valid.any() else None,
                        'metoda':'APROKSYMACJA_LINIOWA_BEZ_PONOWNEJ_SYMULACJI',
                        'ograniczenie':'Nie przelicza fill/exit/TP/SL ani finansowania; bez historycznego orderbook; nie uzywac jako dowodu przewagi'})
    return pd.DataFrame(out,columns=cols)


def test_sensitivity_oos_v137():
    sample=pd.DataFrame([{'wariant':'BAZA','r_netto':-1.0,'slip_bps':30.0,'risk_pct':1.0},
                         {'wariant':'BAZA','r_netto':2.0,'slip_bps':30.0,'risk_pct':2.0}])
    x=sensitivity_oos_v137(sample).set_index('scenariusz_slip_bps')
    assert abs(x.loc[30.0,'suma_R_przyblizona']-1.0)<1e-8
    assert abs(x.loc[10.0,'suma_R_przyblizona']-1.3)<1e-8
    assert (x['ocenialne']==2).all()
    assert sensitivity_oos_v137(pd.DataFrame()).empty
    return 'OK (wariant bazowy, liniowy koszt, brak danych)'


def runtime_identity_v138():
    """Hash only an actual executing source file; no guessed file identity for exec()."""
    path=globals().get('__file__')
    expected=f'weex_v6_{VERSION.split(".")[-1]}.py'
    try:
        if globals().get('__name__')=='__main__' and isinstance(path,str):
            source=Path(path).resolve(strict=True)
            raw=source.read_bytes()
            if (source.is_file() and
                ("VERSION = '"+VERSION+"'").encode('utf-8') in raw):
                return {'skrypt_sha256':hashlib.sha256(raw).hexdigest(),
                        'skrypt_hash_status':'PLIK_MAIN_ZWERYFIKOWANY_WERSJA_W_TRESCI',
                        'sciezka_weryfikowana':str(source),
                        'wersja_skryptu':VERSION}
    except (OSError,ValueError,TypeError):
        pass
    return {'skrypt_sha256':None,'skrypt_hash_status':'NIEZWERYFIKOWANY_BRAK_ZGODNEJ_SCIEZKI',
            'zalecany_sposob_uruchomienia':f'%run /content/{expected}',
            'wersja_skryptu':VERSION}


# V6.141: audit the actual funding timestamp series, without affecting TTL or trading.
def funding_chronologia_v141(before, after, now_ms, fetched, status):
    def examine(hist):
        vals=[]; invalid=0
        for row in hist if isinstance(hist,list) else []:
            try:
                t=int(number(row.get('fundingTime')) or 0) if isinstance(row,dict) else 0
                if t<=0: invalid+=1
                else: vals.append(t)
            except (ValueError,TypeError,OverflowError):
                invalid+=1
        unique=sorted(set(vals))
        gaps=[b-a for a,b in zip(unique,unique[1:])]
        # Timestamp ordering depends on API; inspect it, do not assume ascending order.
        order=('ROSNACA' if vals==sorted(vals) else 'MALEJACA' if vals==sorted(vals,reverse=True)
               else 'NIEUPORZADKOWANA') if len(vals)>1 else 'BRAK_DANYCH'
        positive=[g for g in gaps if g>0]
        typical=float(np.median(positive[-5:])) if positive else None
        unusual=sum(1 for g in positive if typical and abs(g-typical)>max(60000,typical*0.25))
        return dict(liczba=len(vals),niepoprawne=invalid,duplikaty=len(vals)-len(unique),
                    kolejnosc_api=order,ostatni_ms=max(unique,default=None),
                    pierwszy_ms=min(unique,default=None),typowy_odstep_ms=typical,
                    luki_niestandardowe=unusual)
    old=examine(before); new=examine(after)
    oldlast=old['ostatni_ms']; newlast=new['ostatni_ms']
    progress=(newlast-oldlast) if newlast is not None and oldlast is not None else None
    if not fetched: category='BRAK_PROBY_API'
    elif status=='BLAD_API': category='BLAD_API'
    elif newlast is None: category='BRAK_POPRAWNYCH_DAT_Z_API'
    elif oldlast is None: category='PIERWSZA_HISTORIA'
    elif newlast>oldlast: category='NOWSZA_STAWKA'
    elif newlast<oldlast: category='COFNIECIE_OSTATNIEJ_STAWKI'
    elif status=='ZMIANA_TRESCI_BEZ_NOWSZEJ_STAWKI': category='KOREKTA_STAREJ_HISTORII'
    else: category='BRAK_NOWSZEJ_STAWKI'
    inferred=old['typowy_odstep_ms']
    # Strong separation between observed API changes and inferred publication time.
    drift=(now_ms-(oldlast+inferred)) if inferred and oldlast else None
    return {'symbol':None,'czy_pobrano_api':bool(fetched),'status_chronologii':category,
            'ostatnia_przed_ms':oldlast,'ostatnia_po_ms':newlast,
            'postep_ms':progress,'szacowany_cykl_ms':inferred,
            'odchylenie_od_szacowanego_terminu_ms':drift,
            'rzeczywisty_cykl_ostatnich_stawek_ms':new['typowy_odstep_ms'],
            'cykl_roznica_ms':(new['typowy_odstep_ms']-inferred) if new['typowy_odstep_ms'] and inferred else None,
            'api_kolejnosc':new['kolejnosc_api'],'api_duplikaty':new['duplikaty'],
            'api_niepoprawne_timestamps':new['niepoprawne'],
            'api_nietypowe_odstepy':new['luki_niestandardowe'],
            'oficjalny_harmonogram_zweryfikowany':False,
            'ograniczenie':'Wyniki na podstawie historii API, nie dowod zmiany oficjalnego harmonogramu'}


# V6.142: distribution of observed intervals. No modification of cache TTL.
def funding_odstepy_v142(history, symbol=None):
    valid=[]; invalid=0
    for row in history if isinstance(history,list) else []:
        try:
            value=int(number(row.get('fundingTime')) or 0) if isinstance(row,dict) else 0
            if value>0: valid.append(value)
            else: invalid+=1
        except (ValueError,TypeError,OverflowError): invalid+=1
    unique=sorted(set(valid))
    gap_h=[(b-a)/3600000.0 for a,b in zip(unique,unique[1:]) if b>a]
    # Historical gaps can represent missing records rather than a real funding cycle.
    freq={}
    for gap in gap_h:
        label=f'{round(gap,3):g}h'
        freq[label]=freq.get(label,0)+1
    mode=(sorted(freq.items(),key=lambda x:(-x[1],float(x[0][:-1])))[0][0]
          if freq else None)
    med=float(np.median(gap_h)) if gap_h else None
    return {'symbol':symbol,'rekordy':len(valid),'unikalne_stawki':len(unique),
            'duplikaty':len(valid)-len(unique),'bledne_timestamps':invalid,
            'odstepow':len(gap_h),'mediana_odstepu_h':med,
            'najczestszy_odstep_h':mode,'min_odstep_h':min(gap_h,default=None),
            'max_odstep_h':max(gap_h,default=None),
            'rozkład_odstepow_json':json.dumps(freq,sort_keys=True,ensure_ascii=False),
            'odstepy_powyzej_1_5x_mediany':sum(v>1.5*med for v in gap_h) if med else 0,
            'uwaga':'Luki moga oznaczac brakujace rekordy, a nie zmiane oficjalnego cyklu'}


def test_funding_odstepy_v142():
    base=1793000000000
    hist=[{'fundingTime':str(base+v*3600000)} for v in (0,1,2,6,7)]
    r=funding_odstepy_v142(hist,'TESTUSDT')
    assert r['odstepow']==4 and r['najczestszy_odstep_h']=='1h'
    assert json.loads(r['rozkład_odstepow_json'])=={'1h':3,'4h':1}
    assert r['odstepy_powyzej_1_5x_mediany']==1
    assert funding_odstepy_v142([], 'EMPTY')['odstepow']==0
    return 'OK (histogram, luka, pusty zbior)'


# V6.143: recent funding regimes are diagnostic only; neither TTL nor trading is affected.
def funding_ostatnie_cykle_v143(history, symbol=None, windows=(10,20)):
    stamps=[]; bad=0
    for row in history if isinstance(history,list) else []:
        try:
            t=int(number(row.get('fundingTime')) or 0) if isinstance(row,dict) else 0
            if t>0: stamps.append(t)
            else: bad+=1
        except (ValueError,TypeError,OverflowError): bad+=1
    uniq=sorted(set(stamps))
    gaps=[round((b-a)/3600000.0,6) for a,b in zip(uniq,uniq[1:]) if b>a]
    out=[]
    for n in windows:
        recent=gaps[-n:]
        count1=sum(abs(v-1)<0.05 for v in recent)
        count4=sum(abs(v-4)<0.05 for v in recent)
        other=len(recent)-count1-count4
        older=gaps[-2*n:-n] if len(gaps)>n else []
        prior1=sum(abs(v-1)<0.05 for v in older)
        prior4=sum(abs(v-4)<0.05 for v in older)
        now_regime=('DOMINUJE_1H' if len(recent)>=max(5,n//2) and count1/len(recent)>=0.8 else
                    'DOMINUJE_4H' if len(recent)>=max(5,n//2) and count4/len(recent)>=0.8 else
                    'MIESZANE_LUB_NIEPEWNE' if recent else 'BRAK_DANYCH')
        old_regime=('DOMINUJE_1H' if older and prior1/len(older)>=0.8 else
                    'DOMINUJE_4H' if older and prior4/len(older)>=0.8 else 'NIEUSTALONE')
        possible_change= (old_regime in ('DOMINUJE_1H','DOMINUJE_4H') and
                          now_regime in ('DOMINUJE_1H','DOMINUJE_4H') and old_regime != now_regime and
                          len(older)>=max(5,n//2))
        out.append({'symbol':symbol,'okno_odstepow':n,'liczba_odstepow':len(recent),
                    'odstepy_1h':count1,'odstepy_4h':count4,'odstepy_inne':other,
                    'mediana_h':float(np.median(recent)) if recent else None,
                    'poprzednie_okno_liczba':len(older),'poprzednie_okno_1h':prior1,
                    'poprzednie_okno_4h':prior4,'poprzedni_rezim':old_regime,
                    'ostatni_rezim':now_regime,
                    'mozliwa_zmiana_cyklu':bool(possible_change),
                    'mozliwe_luki_lub_inny_cykl':bool(other or (count1 and count4)),
                    'duplikaty':len(stamps)-len(uniq),'bledne_timestamps':bad,
                    'ostatnia_stawka_ms':uniq[-1] if uniq else None,
                    'oficjalny_harmonogram_potwierdzony':False,
                    'uwaga':'Odstepy i ewentualna zmiana rezimu sa tylko obserwacja historii; luki moga imitowac zmiane cyklu'})
    return out


# V6.144: individual historical intervals and conservative diagnostic classification.
# A missing single record cannot independently establish an exchange schedule change.
def funding_odstepy_szczegoly_v144(history, symbol=None, limit=20):
    valid=[]
    for row in history if isinstance(history,list) else []:
        if not isinstance(row,dict): continue
        try:
            stamp=int(number(row.get('fundingTime')) or 0)
            if stamp>0: valid.append(stamp)
        except (ValueError,TypeError,OverflowError): pass
    stamps=sorted(set(valid))
    pairs=list(zip(stamps,stamps[1:]))[-limit:]
    rows=[]
    for a,b in pairs:
        gap=round((b-a)/3600000,6)
        label=('ODSTEP_1H' if abs(gap-1)<0.05 else
               'ODSTEP_4H_NIEJEDNOZNACZNY' if abs(gap-4)<0.05 else
               'INNY_ODSTEP_NIEJEDNOZNACZNY')
        rows.append({'symbol':symbol,'poczatek_utc':iso(a),'koniec_utc':iso(b),
                     'poczatek_ms':a,'koniec_ms':b,'odstep_h':gap,'klasyfikacja':label,
                     'potwierdzona_zmiana_harmonogramu':False,
                     'uwaga':'Odstęp może wynikać z brakującej stawki lub zmiany cyklu; sam w sobie nie rozstrzyga'})
    return rows


def funding_rezim_ocena_v144(rows):
    if not rows: return {'status':'BRAK_DANYCH','zmiana_potwierdzona':False}
    recent=rows[-10:]
    four=sum(r['klasyfikacja']=='ODSTEP_4H_NIEJEDNOZNACZNY' for r in recent)
    one=sum(r['klasyfikacja']=='ODSTEP_1H' for r in recent)
    if len(recent)>=8 and four>=8:
        status='SERIA_4H_MOZLIWA_ZMIANA_LUB_LUKI'
    elif four or any(r['klasyfikacja']=='INNY_ODSTEP_NIEJEDNOZNACZNY' for r in recent):
        status='POJEDYNCZE_NIEJEDNOZNACZNE_ODSTEPY'
    elif one==len(recent): status='REGULARNE_1H'
    else: status='BRAK_POTWIERDZENIA'
    return {'status':status,'zmiana_potwierdzona':False,'liczba_1h':one,'liczba_4h':four,'okno':len(recent)}


# V6.145 — strictly diagnostic regime comparison; stable 4h is not a change.
def funding_rezim_v145(history, symbol=None, window=10):
    stamps=[]; invalid=0
    for r in history if isinstance(history,list) else []:
        try:
            v=int(number(r.get('fundingTime')) or 0) if isinstance(r,dict) else 0
            if v>0: stamps.append(v)
            else: invalid+=1
        except (ValueError,TypeError,OverflowError): invalid+=1
    unique=sorted(set(stamps))
    gaps=[(b-a)/3600000.0 for a,b in zip(unique,unique[1:])]
    recent=gaps[-window:]
    earlier=gaps[-2*window:-window] if len(gaps)>=2*window else []
    def classify(vals):
        if len(vals)<max(5,window//2): return ('BRAK_PROBY',0,0,0)
        n1=sum(abs(x-1)<=0.05 for x in vals)
        n4=sum(abs(x-4)<=0.05 for x in vals)
        other=len(vals)-n1-n4
        regime=('STABILNY_1H' if n1/len(vals)>=0.8 else
                'STABILNY_4H' if n4/len(vals)>=0.8 else 'MIESZANY')
        return regime,n1,n4,other
    new,c1,c4,co=classify(recent)
    old,p1,p4,po=classify(earlier)
    change=(old in ('STABILNY_1H','STABILNY_4H') and
            new in ('STABILNY_1H','STABILNY_4H') and old!=new)
    status=('MOZLIWA_ZMIANA_CYKLU' if change else
            'NIEJEDNOZNACZNE_LUKI_LUB_CYKL' if new=='MIESZANY' else
            new if new!='BRAK_PROBY' else 'BRAK_DANYCH')
    return dict(symbol=symbol,status=status,poprzedni_rezim=old,obecny_rezim=new,
        mozliwa_zmiana_cyklu=bool(change),zmiana_potwierdzona=False,
        ostatnie_odstepy=len(recent),poprzednie_odstepy=len(earlier),
        obecne_1h=c1,obecne_4h=c4,obecne_inne=co,
        poprzednie_1h=p1,poprzednie_4h=p4,poprzednie_inne=po,
        potencjalne_luki=bool(co or po or (c1 and c4) or (p1 and p4)),
        duplikaty=len(stamps)-len(unique),bledne_timestamps=invalid,
        ostatnia_stawka_ms=unique[-1] if unique else None,
        uwaga='Historia nie potwierdza oficjalnego harmonogramu; pojedyncze odstepy 4h moga oznaczac brak danych')


def test_funding_v145():
    base=1793000000000
    def hist(gaps):
        z=[base]
        for h in gaps:z.append(z[-1]+h*3600000)
        return [{'fundingTime':str(t)} for t in z[::-1]]
    stable=funding_rezim_v145(hist([4]*30),'STABLE')
    assert stable['status']=='STABILNY_4H' and not stable['mozliwa_zmiana_cyklu']
    shift=funding_rezim_v145(hist([1]*15+[4]*12),'SHIFT')
    assert shift['status']=='MOZLIWA_ZMIANA_CYKLU' and shift['poprzedni_rezim']=='STABILNY_1H'
    hole=funding_rezim_v145(hist([1]*20+[1]*4+[4]+[1]*5),'HOLE')
    assert hole['obecny_rezim']=='STABILNY_1H' and not hole['mozliwa_zmiana_cyklu'] and hole['potencjalne_luki']
    assert funding_rezim_v145([])['status']=='BRAK_DANYCH'
    return 'OK (stabilny 4h, zmiana 1h na 4h, pojedyncza luka, brak historii)'


def runtime_identity_v145():
    result=runtime_identity_v142()
    expected=os.environ.get('WEEX_EXPECTED_SHA256','').strip().lower()
    actual=result.get('skrypt_sha256')
    result['oczekiwany_sha256']=expected or None
    result['porownanie_hash']=(('ZGODNY_Z_HASHEM_PRZED_URUCHOMIENIEM' if actual==expected else
        'NIEZGODNY_Z_HASHEM_PRZED_URUCHOMIENIEM') if expected and actual else
        'NIEZWERYFIKOWANE_BRAK_DOWODU')
    result['uwaga_v145']='Weryfikuje bajty wskazanego pliku. Samo exec() nie jest dowodem uruchomienia tych bajtow.'
    return result


def test_runtime_identity_v145():
    import tempfile
    with tempfile.TemporaryDirectory() as d:
        file=Path(d)/'changed_name.py'
        file.write_text("VERSION = '"+VERSION+"'\n",encoding='utf-8')
        digest=hashlib.sha256(file.read_bytes()).hexdigest()
        old_path=os.environ.get('WEEX_EXECUTED_FILE')
        old_hash=os.environ.get('WEEX_EXPECTED_SHA256')
        try:
            os.environ['WEEX_EXECUTED_FILE']=str(file)
            os.environ['WEEX_EXPECTED_SHA256']=digest
            assert runtime_identity_v145()['porownanie_hash']=='ZGODNY_Z_HASHEM_PRZED_URUCHOMIENIEM'
            os.environ['WEEX_EXPECTED_SHA256']='0'*64
            assert runtime_identity_v145()['porownanie_hash']=='NIEZGODNY_Z_HASHEM_PRZED_URUCHOMIENIEM'
        finally:
            for key,value in [('WEEX_EXECUTED_FILE',old_path),('WEEX_EXPECTED_SHA256',old_hash)]:
                if value is None:os.environ.pop(key,None)
                else:os.environ[key]=value
    return 'OK (hash zgodny, hash niezgodny, zmieniona nazwa)'


def test_funding_v144():
    base=1793000000000
    stamps=[base+i*3600000 for i in range(11) if i not in (5,6,7)]
    rows=funding_odstepy_szczegoly_v144([{'fundingTime':str(t)} for t in reversed(stamps)],'TEST')
    assert len(rows)==7 and sum(r['klasyfikacja']=='ODSTEP_4H_NIEJEDNOZNACZNY' for r in rows)==1
    assert funding_rezim_ocena_v144(rows)['status']=='POJEDYNCZE_NIEJEDNOZNACZNE_ODSTEPY'
    assert all(not r['potwierdzona_zmiana_harmonogramu'] for r in rows)
    assert not funding_odstepy_szczegoly_v144([], 'EMPTY')
    assert funding_rezim_ocena_v144([])['status']=='BRAK_DANYCH'
    return 'OK (daty UTC, pojedyncza luka, brak danych, brak falszywego potwierdzenia)'


def runtime_identity_v144():
    identity=runtime_identity_v142()
    identity['identyfikacja_v144']='HASH_PLIKU_LUB_NIEZWERYFIKOWANY_ZALEZNIE_OD_METODY_URUCHOMIENIA'
    identity['uwaga']='Hash pliku nie jest niezaleznym dowodem bajtow wykonanych przez exec()'
    return identity

def test_funding_ostatnie_cykle_v143():
    base=1_793_000_000_000
    vals=[base]
    for gap in [3600000]*25+[4*3600000]*20:
        vals.append(vals[-1]+gap)
    hist=[{'fundingTime':str(x)} for x in reversed(vals)]
    rows=funding_ostatnie_cykle_v143(hist,'TEST')
    a={r['okno_odstepow']:r for r in rows}
    assert a[10]['ostatni_rezim']=='DOMINUJE_4H'
    assert a[20]['mozliwa_zmiana_cyklu'] and a[20]['poprzedni_rezim']=='DOMINUJE_1H'
    assert all(not r['oficjalny_harmonogram_potwierdzony'] for r in rows)
    assert all(r['ostatni_rezim']=='BRAK_DANYCH' for r in funding_ostatnie_cykle_v143([], 'EMPTY'))
    # Missing record can create a 4h gap, without proving a new official cycle.
    g=[base+i*3600000 for i in range(11) if i not in (5,6,7)]
    b=funding_ostatnie_cykle_v143([{'fundingTime':str(x)} for x in g])[0]
    assert b['mozliwe_luki_lub_inny_cykl']
    return 'OK (1h->4h, okna 10/20, luki, brak danych)'


def runtime_identity_v142(explicit_path=None):
    """Explicit opt-in path; verification means file bytes match selected path, not exec source proof."""
    source_path=explicit_path or globals().get('WEEX_EXECUTED_FILE') or __import__('os').environ.get('WEEX_EXECUTED_FILE')
    if source_path:
        try:
            src=Path(source_path).expanduser().resolve(strict=True)
            raw=src.read_bytes()
            if not src.is_file(): raise ValueError('not file')
            if ("VERSION = '"+VERSION+"'").encode('utf-8') not in raw:
                raise ValueError('niezgodna wersja pliku')
            return {'skrypt_sha256':hashlib.sha256(raw).hexdigest(),
                    'skrypt_hash_status':'ZWERYFIKOWANO_WSKAZANY_PLIK_NIE_SPOSOB_EXEC',
                    'sciezka_weryfikowana':str(src),'wersja_skryptu':VERSION,
                    'ograniczenie':'Jawna sciezka potwierdza zawartosc pliku, nie dowodzi ktore bajty uruchomil exec()'}
        except (OSError,ValueError,TypeError) as exc:
            return {'skrypt_sha256':None,'skrypt_hash_status':'BLEDNA_JAWNA_SCIEZKA_LUB_WERSJA',
                    'blad':str(exc)[:160],'wersja_skryptu':VERSION}
    return runtime_identity_v138()


def test_runtime_identity_v142():
    import tempfile
    with tempfile.TemporaryDirectory() as folder:
        f=Path(folder)/'renamed.py'; f.write_text("VERSION = '"+VERSION+"'\n",encoding='utf8')
        v=runtime_identity_v142(str(f))
        assert len(v['skrypt_sha256'])==64 and 'WSKAZANY_PLIK' in v['skrypt_hash_status']
        f.write_text("VERSION = 'V6.999'\n",encoding='utf8')
        assert runtime_identity_v142(str(f))['skrypt_sha256'] is None
    return 'OK (jawna nazwa, hash, zgodnosc wersji)'


def test_funding_chronologia_v141():
    base=1_793_000_000_000
    h=[{'fundingTime':str(base+i*3600000),'fundingRate':'0.001'} for i in range(8)]
    same=funding_chronologia_v141(h,list(reversed(h)),base+8*3600000+33000,True,'API_BEZ_POSTEPU')
    assert same['status_chronologii']=='BRAK_NOWSZEJ_STAWKI' and same['api_kolejnosc']=='MALEJACA'
    assert same['odchylenie_od_szacowanego_terminu_ms']==33000
    nxt=funding_chronologia_v141(h,h+[{'fundingTime':str(base+8*3600000)}],base+8*3600000,True,'NOWSZA_STAWKA')
    assert nxt['postep_ms']==3600000 and nxt['status_chronologii']=='NOWSZA_STAWKA'
    assert funding_chronologia_v141(h,h,base,False,'CACHE_BEZ_ODSWIEZENIA')['status_chronologii']=='BRAK_PROBY_API'
    assert funding_chronologia_v141(h,[],base,True,'BLAD_API')['status_chronologii']=='BLAD_API'
    dupe=funding_chronologia_v141(h,h+[dict(h[-1])],base,True,'API_BEZ_POSTEPU')
    assert dupe['api_duplikaty']==1
    return 'OK (nowa stawka, cache, blad, duplikaty, kolejnosc, opoznienie)'


def funding_opoznienie_publikacji_v140(history, now_ms, status, streak=0):
    """Klasyfikuje opóźnienie szacowanej publikacji bez zmiany TTL i zapytań API.

    Wiek liczony od szacowanej daty, a nie od daty wykonania zapytania.
    Brak wiarygodnego interwału oznacza NIEUSTALONE, a nie alarm API.
    """
    diag = funding_cache_diagnostyka_v138(history, now_ms)
    due = diag.get('nastepny_szacowany_ms')
    lag = max(0, now_ms - int(due)) if due is not None else None
    no_progress = status == 'API_BEZ_POSTEPU'
    if not no_progress:
        kind = 'BRAK_BRAKU_POSTEPU' if status != 'BLAD_API' else 'BLAD_API_OSOBNO'
    elif lag is None:
        kind = 'BRAK_HARMONOGRAMU_DO_OCENY'
    elif lag <= 2 * 60 * 1000:
        kind = 'KROTKIE_OPOZNIENIE_DO_2_MIN'
    elif lag <= 15 * 60 * 1000:
        kind = 'OPOZNIENIE_2_DO_15_MIN'
    elif int(streak) >= 2:
        kind = 'POWTARZAJACY_SIE_BRAK_POSTEPU_PONAD_15_MIN'
    else:
        kind = 'BRAK_POSTEPU_PONAD_15_MIN'
    return {'klasa_opoznienia':kind, 'opoznienie_od_szacowanej_stawki_ms':lag,
            'nastepny_szacowany_ms':due,
            'cykl_typowy_ms':diag.get('odstep_typowy_ms'),
            'harmonogram_potwierdzony_przez_gielde':False,
            'uwaga':'Szacunek z historii; nie zmienia waznosci cache ani zapytan API'}


def test_funding_opoznienie_v140():
    base = 1_793_000_000_000
    hist = [{'fundingTime':str(base + i*3600000), 'fundingRate':'0.001'} for i in range(8)]
    due = base + 8*3600000
    assert funding_opoznienie_publikacji_v140(hist,due+33000,'API_BEZ_POSTEPU',1)['klasa_opoznienia'] == 'KROTKIE_OPOZNIENIE_DO_2_MIN'
    assert funding_opoznienie_publikacji_v140(hist,due+5*60000,'API_BEZ_POSTEPU',1)['klasa_opoznienia'] == 'OPOZNIENIE_2_DO_15_MIN'
    assert funding_opoznienie_publikacji_v140(hist,due+20*60000,'API_BEZ_POSTEPU',2)['klasa_opoznienia'] == 'POWTARZAJACY_SIE_BRAK_POSTEPU_PONAD_15_MIN'
    assert funding_opoznienie_publikacji_v140(hist,due+20*60000,'NOWSZA_STAWKA',0)['klasa_opoznienia'] == 'BRAK_BRAKU_POSTEPU'
    assert funding_opoznienie_publikacji_v140([],due+20*60000,'API_BEZ_POSTEPU',2)['klasa_opoznienia'] == 'BRAK_HARMONOGRAMU_DO_OCENY'
    assert funding_aktualny(hist,due-120001) and not funding_aktualny(hist,due-120000)
    return 'OK (33 s, 5 min, 20 min, nowa stawka, brak historii, TTL bez zmian)'


# V6.146: tylko raportowanie; nie zmienia selekcji ani terminu waznosci cache.
def funding_payload_audit_v146(before, after, fetched, error=''):
    """Porownanie calych normalizowanych serii (timestamp, rate), niezalezne od kolejnosci API.

    Powtorzone timestampy z rozna stawka sa jawnie niejednoznaczne.
    Rozne okna API nie dowodza korekty starszych danych; liczymy tylko wspolne timestampy.
    """
    def parse(history):
        rates = {}
        bad = 0
        ambiguous = set()
        if not isinstance(history, list):
            return rates, bad, ambiguous
        for item in history:
            if not isinstance(item, dict):
                bad += 1
                continue
            try:
                stamp = int(str(item.get('fundingTime', '')).strip())
                rate = Decimal(str(item.get('fundingRate', '')).strip())
                if stamp <= 0 or not rate.is_finite():
                    raise ValueError('invalid funding')
                val = format(rate.normalize(), 'f')
            except (ValueError, TypeError, ArithmeticError):
                bad += 1
                continue
            if stamp in rates and rates[stamp] != val:
                ambiguous.add(stamp)
            rates[stamp] = val
        return rates, bad, ambiguous
    old, bad_old, dup_old = parse(before)
    new, bad_new, dup_new = parse(after)
    shared = set(old) & set(new)
    corrected = sorted(t for t in shared if old[t] != new[t] and t not in (dup_old | dup_new))
    added = sorted(set(new) - set(old))
    missing = sorted(set(old) - set(new))
    ambiguous_shared = sorted(shared & (dup_old | dup_new))
    if not fetched:
        status = 'CACHE_BEZ_API'
    elif error:
        status = 'BLAD_API'
    elif not new:
        status = 'BRAK_POPRAWNEJ_ODPOWIEDZI'
    elif ambiguous_shared:
        status = 'NIEJEDNOZNACZNE_DUPLIKATY'
    elif corrected:
        status = 'KOREKTA_WSPOLNYCH_REKORDOW'
    elif added and max(added) > max(old, default=0):
        status = 'NOWSZE_REKORDY'
    elif old == new:
        status = 'IDENTYCZNA_SERIA'
    elif missing or added:
        status = 'INNE_OKNO_HISTORII_API'
    else:
        status = 'BEZ_ZMIAN_WSPOLNYCH'
    return {'status_pelnej_serii':status,'stare_rekordy':len(old),'nowe_rekordy':len(new),
            'wspolne_rekordy':len(shared),'korekty_wspolnych':len(corrected),
            'nowe_timestampy':len(added),'znikniete_timestampy':len(missing),
            'bledne_stare':bad_old,'bledne_nowe':bad_new,
            'niejednoznaczne_duplikaty':len(dup_old | dup_new),
            'stary_ostatni_ms':max(old,default=None),'nowy_ostatni_ms':max(new,default=None),
            'hash_serii_starej':hashlib.sha256(json.dumps(sorted(old.items())).encode()).hexdigest() if old else None,
            'hash_serii_nowej':hashlib.sha256(json.dumps(sorted(new.items())).encode()).hexdigest() if new else None,
            'zmienione_timestampy_json':json.dumps(corrected[:50]),
            'uwaga':'Zestawienie wspolnych rekordow; zmiana okna API nie musi oznaczac korekty historii'}


def runtime_manifest_v146():
    """SHA wskazanego pliku; bez niezaleznego dowodu wykonanych bajtow."""
    d = runtime_identity_v145()
    d['manifest_schema'] = 1
    d['version'] = VERSION
    d['created_utc'] = utc_now()
    d['sha256_potwierdza_wskazany_plik_nie_exec'] = True
    d['czy_hash_zgodny'] = d.get('porownanie_hash') == 'ZGODNY_Z_HASHEM_PRZED_URUCHOMIENIEM'
    return d


def test_v146():
    b=[{'fundingTime':'1000','fundingRate':'0.0100'}, {'fundingTime':'2000','fundingRate':'0.02'}]
    a=list(reversed(b))
    assert funding_payload_audit_v146(b,a,True)['status_pelnej_serii']=='IDENTYCZNA_SERIA'
    assert funding_payload_audit_v146(b,[{'fundingTime':'1000','fundingRate':'0.011'},{'fundingTime':'2000','fundingRate':'0.02'}],True)['status_pelnej_serii']=='KOREKTA_WSPOLNYCH_REKORDOW'
    assert funding_payload_audit_v146(b,a+[{'fundingTime':'3000','fundingRate':'0.03'}],True)['status_pelnej_serii']=='NOWSZE_REKORDY'
    assert funding_payload_audit_v146(b,a[0:1],True)['status_pelnej_serii']=='INNE_OKNO_HISTORII_API'
    assert funding_payload_audit_v146(b,a,False)['status_pelnej_serii']=='CACHE_BEZ_API'
    return 'OK (identyczne, korekta, nowsze, inne okno, cache)'



# V6.147 — report-only: status instrumentu i diagnostyka starych historii.
def funding_status_instrumentu_v147(meta, ticker):
    """Pola wyłącznie z istniejących odpowiedzi exchangeInfo/ticker.

    Brak statusu nie oznacza ACTIVE. Nie zakładamy oficjalnego cyklu fundingu.
    """
    m=meta if isinstance(meta, dict) else {}
    t=ticker if isinstance(ticker, dict) else {}
    keys=('status','symbolStatus','contractStatus','state','contractType',
          'onboardDate','deliveryDate','fundingInterval','fundingIntervalHours',
          'fundingIntervalMinutes','nextFundingTime')
    return {'exchangeInfo_'+k:m.get(k) for k in keys if k in m} | {
           'ticker_'+k:t.get(k) for k in keys if k in t}


def funding_diagnostyka_starej_serii_v147(symbol, coin, full_series, progress, now_ms):
    meta=(coin or {}).get('meta') or {}
    ticker=(coin or {}).get('ticker') or {}
    status=str(full_series.get('status_pelnej_serii') or '')
    streak=int(progress.get('kolejne_odswiezenia_bez_postepu') or 0)
    old_ms=full_series.get('stary_ostatni_ms')
    new_ms=full_series.get('nowy_ostatni_ms')
    last_ms=new_ms if new_ms is not None else old_ms
    age_h=(round((int(now_ms)-int(last_ms))/3600000,3)
           if last_ms is not None else None)
    if status=='IDENTYCZNA_SERIA' and streak>=2:
        diagnostic='POWTARZANA_IDENTYCZNA_ODPOWIEDZ'
    elif status=='IDENTYCZNA_SERIA':
        diagnostic='IDENTYCZNA_ODPOWIEDZ_PIERWSZA'
    elif status=='NOWSZE_REKORDY':
        diagnostic='API_DOSTARCZYLO_NOWE_STAWKI'
    elif status=='KOREKTA_WSPOLNYCH_REKORDOW':
        diagnostic='KOREKTA_HISTORYCZNYCH_STAWEK'
    elif status=='CACHE_BEZ_API':
        diagnostic='BRAK_ZAPYTANIA_CACHE'
    else:
        diagnostic='WYMAGA_OCENY_'+status
    return {'symbol':symbol,'status_diagnostyczny':diagnostic,
            'kolejne_bez_postepu':streak,'ostatni_funding_utc':iso(last_ms) if last_ms else None,
            'wiek_ostatniej_stawki_h':age_h,
            'api_seria_hash_taki_sam':bool(full_series.get('hash_serii_starej') and
                                          full_series.get('hash_serii_starej')==full_series.get('hash_serii_nowej')),
            'weryfikacja_harmonogramu':'NIEPOTWIERDZONO_OFICJALNEGO_TERMINU',
            'kontrakt_wg_exchangeInfo':str(meta.get('status',meta.get('symbolStatus',meta.get('contractStatus','NIEPODANO')))),
            'kontrakt_wg_ticker':str(ticker.get('status',ticker.get('symbolStatus','NIEPODANO'))),
            'status_i_parametry_z_API':json.dumps(funding_status_instrumentu_v147(meta,ticker),ensure_ascii=False,sort_keys=True,default=str),
            'uwaga':'Raport diagnostyczny. Nie wydluza TTL, nie blokuje odswiezen i nie potwierdza oficjalnego cyklu.'}


def runtime_manifest_v147():
    """Powiąż identyfikator raportu z zawartością pliku; nie udawaj dowodu wykonania."""
    d=runtime_manifest_v146()
    d['manifest_schema']=2
    d['version']=VERSION
    d['hash_wskazanego_pliku']=d.get('skrypt_sha256')
    d['oczekiwany_hash_przed_uruchomieniem']=d.get('oczekiwany_sha256')
    d['czy_hash_jest_dowodem_wykonania']=False
    d['status_zaufania']='ZGODNY_WSKAZANY_PLIK' if d.get('czy_hash_zgodny') else 'BRAK_WERYFIKACJI_ZGODNOSCI'
    d['instrukcja']='Ustaw WEEX_EXECUTED_FILE i WEEX_EXPECTED_SHA256 przed %run; samo exec() nie potwierdza uruchomionych bajtow.'
    return d


def test_v147():
    base={'status_pelnej_serii':'IDENTYCZNA_SERIA','hash_serii_starej':'ab','hash_serii_nowej':'ab',
          'nowy_ostatni_ms':1700000000000,'stary_ostatni_ms':1700000000000}
    coin={'meta':{'status':'TRADING','contractType':'PERPETUAL'},'ticker':{}}
    d=funding_diagnostyka_starej_serii_v147('IOSTUSDT',coin,base,
            {'kolejne_odswiezenia_bez_postepu':10},1700003600000)
    assert d['status_diagnostyczny']=='POWTARZANA_IDENTYCZNA_ODPOWIEDZ'
    assert d['api_seria_hash_taki_sam'] and d['kontrakt_wg_exchangeInfo']=='TRADING'
    assert d['wiek_ostatniej_stawki_h']==1.0
    assert funding_diagnostyka_starej_serii_v147('X',{},
        {**base,'status_pelnej_serii':'NOWSZE_REKORDY'},{},1700003600000)['status_diagnostyczny']=='API_DOSTARCZYLO_NOWE_STAWKI'
    assert not runtime_manifest_v147()['czy_hash_jest_dowodem_wykonania']
    return 'OK (powtarzane API, instrument, nowe dane, manifest)'

# V6.148 — diagnostyka brakujacych statusow, powtarzanych danych i manifestu.
def funding_status_dostepnosc_v148(coin, diag):
    meta=(coin or {}).get('meta') or {}
    ticker=(coin or {}).get('ticker') or {}
    keys=('status','symbolStatus','contractStatus','state')
    entries=[(source,key,value) for source,obj in (('exchangeInfo',meta),('ticker',ticker))
             for key in keys for value in [obj.get(key)] if value is not None and str(value).strip()]
    type_contract=str(meta.get('contractType') or '')
    streak=int(diag.get('kolejne_bez_postepu') or 0)
    unchanged=diag.get('status_diagnostyczny')=='POWTARZANA_IDENTYCZNA_ODPOWIEDZ' and bool(diag.get('api_seria_hash_taki_sam'))
    return {'status_handlowy_dostepnosc': 'DOSTEPNY_W_API' if entries else 'STATUS_NIEDOSTEPNY_W_API',
            'status_handlowy_pola':json.dumps([{'zrodlo':a,'pole':b,'wartosc':str(c)} for a,b,c in entries],ensure_ascii=False),
            'typ_kontraktu_nie_jest_statusem': type_contract or None,
            'ostrzezenie_powtarzanej_serii': 'POWTARZANA_SERIA_5_PLUS_BEZ_NOWYCH_STAWEK' if unchanged and streak>=5 else
              'POWTARZANA_SERIA_2_PLUS' if unchanged else 'BRAK_POTWIERDZONEJ_SERII',
            'status_rynku_potwierdzony':bool(entries),
            'funding_aktualny_potwierdzony':False if unchanged else None,
            'uwaga_v148':'Brak pola statusu nie oznacza aktywnego rynku; brak nowej stawki nie przedluza TTL.'}


def runtime_manifest_v148():
    identity=runtime_manifest_v147()
    expected=os.environ.get('WEEX_EXPECTED_SHA256','').strip().lower()
    file_hash=identity.get('hash_wskazanego_pliku') or identity.get('skrypt_sha256')
    evidence=os.environ.get('WEEX_LAUNCH_SHA256','').strip().lower()
    identity.update(manifest_schema=3, version=VERSION, oczekiwany_sha256=expected or None,
        hash_przed_uruchomieniem=evidence or None,
        hash_z_wykonywanego_pliku=file_hash,
        plik_zgodny_z_oczekiwanym=bool(file_hash and expected and file_hash==expected),
        launcher_hash_zgodny=bool(evidence and file_hash and evidence==file_hash),
        dowod_wykonania_bajtow=False,
        opis='Hash wskazanego pliku jest weryfikacja pliku, a nie niezaleznym dowodem wykonania przez exec.' )
    return identity


def test_v148():
    diag={'kolejne_bez_postepu':11,'status_diagnostyczny':'POWTARZANA_IDENTYCZNA_ODPOWIEDZ','api_seria_hash_taki_sam':True}
    a=funding_status_dostepnosc_v148({'meta':{'contractType':'PERPETUAL'},'ticker':{}},diag)
    assert a['status_handlowy_dostepnosc']=='STATUS_NIEDOSTEPNY_W_API'
    assert not a['status_rynku_potwierdzony'] and '5_PLUS' in a['ostrzezenie_powtarzanej_serii']
    b=funding_status_dostepnosc_v148({'meta':{'status':'TRADING'}},diag)
    assert b['status_handlowy_dostepnosc']=='DOSTEPNY_W_API'
    assert 'TRADING' in b['status_handlowy_pola']
    return 'OK (status niedostepny, brak zalozenia ACTIVE, powtarzane serie)'


def status_przewagi_oos_v149(ocena, do_handlu, potwierdzona=False):
    """Wyłącznie etykieta informacyjna: nigdy nie steruje do_handlu."""
    quality=str(ocena or 'BRAK').strip().upper()
    return {
        'dopuszczenie_techniczne': 'TAK' if do_handlu else 'NIE',
        'przewaga_oos_potwierdzona': 'TAK' if potwierdzona else 'NIE',
        'status_sygnalu_v149': ('PRZEWAGA_OOS_POTWIERDZONA' if potwierdzona else
                               'TECHNICZNIE_DOPUSZCZONY_BEZ_POTWIERDZONEJ_PRZEWAGI' if do_handlu else
                               'OBSERWACJA_LUB_BLOKADA_BEZ_POTWIERDZONEJ_PRZEWAGI'),
        'ostrzezenie_ocena_d_v149': bool(do_handlu and quality=='D'),
        'uwaga_przewaga': 'Ocena i do_handlu nie dowodza dodatniego OOS; sygnal badawczy.'
    }


# V6.172: manifest jawnie rozdziela identyfikacje pliku od dowodu wykonania.
def runtime_manifest_v172():
    d = runtime_manifest_v149()
    d['manifest_schema'] = 5
    d['version'] = VERSION
    d['sposob_uruchomienia'] = ('PLIK_PYTHON' if globals().get('__file__') else 'EXEC_LUB_INTERAKTYWNY')
    d['wskazowka_colab'] = ('Uzyj launchera runpy.run_path z hash SHA256 odczytanym przed startem; '
                           'exec(open(...).read()) nie przekazuje wiarygodnej tozsamosci pliku.')
    d['hash_nie_jest_dowodem_wykonania'] = True
    d['status_weryfikacji_v172'] = d.get('status_weryfikacji_v149', 'NIEZWERYFIKOWANE')
    return d


# V6.173: raport uruchomienia dostarczonego przez launcher Colab.
def runtime_manifest_v173():
    d = runtime_manifest_v172()
    d['manifest_schema'] = 6
    d['version'] = VERSION
    d['launcher_wersja'] = os.environ.get('WEEX_LAUNCHER_VERSION', '')
    d['launcher_przekazal_hash'] = bool(os.environ.get('WEEX_LAUNCH_SHA256'))
    d['status_hash_pliku_v173'] = d.get('status_weryfikacji_v149', 'NIEZWERYFIKOWANE')
    d['uwaga_v173'] = ('Zgodnosc hashy potwierdza wskazany plik, a nie niezaleznie wykonane bajty. '
                      'Brak uruchomienia przez launcher = brak potwierdzenia.')
    return d


# V6.175: niezalezna kontrola spojnosc launchera, wersji i hashy wskazanego pliku.
def runtime_manifest_v175():
    import re
    d = runtime_manifest_v173()
    selected = os.environ.get('WEEX_EXECUTED_FILE', '').strip()
    expected = os.environ.get('WEEX_EXPECTED_SHA256', '').strip().lower()
    evidence = os.environ.get('WEEX_LAUNCH_SHA256', '').strip().lower()
    launcher = os.environ.get('WEEX_LAUNCHER_VERSION', '').strip()
    requested_version = os.environ.get('WEEX_LAUNCH_TARGET_VERSION', '').strip()
    actual_hash = None
    file_version = None
    failure = []
    if not selected:
        failure.append('BRAK_SCIEZKI')
    else:
        try:
            p = Path(selected).resolve(strict=True)
            raw = p.read_bytes()
            actual_hash = hashlib.sha256(raw).hexdigest()
            m = re.search(rb"(?m)^VERSION\s*=\s*['\"](V6\.\d+)['\"]\s*$", raw)
            file_version = m.group(1).decode('ascii') if m else None
            if file_version != VERSION:
                failure.append('WERSJA_PLIKU_NIEZGODNA')
        except (OSError, ValueError) as e:
            failure.append('BLAD_ODCZYTU_PLIKU: '+str(e)[:100])
    if not re.fullmatch(r'[a-f0-9]{64}', expected):
        failure.append('BRAK_LUB_BLEDNY_EXPECTED_SHA')
    if not re.fullmatch(r'[a-f0-9]{64}', evidence):
        failure.append('BRAK_LUB_BLEDNY_LAUNCH_SHA')
    if actual_hash and expected and evidence and not (actual_hash == expected == evidence):
        failure.append('HASH_NIEZGODNY')
    if requested_version != VERSION:
        failure.append('WERSJA_WYBRANA_PRZEZ_LAUNCHER_NIEZGODNA')
    if not launcher.startswith('UNIWERSALNY_'):
        failure.append('NIEPOTWIERDZONY_LAUNCHER')
    d.update(manifest_schema=7, version=VERSION,
        launcher_wersja=launcher, launcher_wersja_docelowa=requested_version,
        skrypt_wersja_odczytana_z_pliku=file_version,
        skrypt_sciezka_uruchomienia=selected or None,
        skrypt_sha256_odczytany=actual_hash,
        skrypt_sha256_oczekiwany=expected or None,
        skrypt_sha256_launcher=evidence or None,
        status_v175='ZGODNE_HASHE_WSKAZANEGO_PLIKU' if not failure else 'BRAK_WERYFIKACJI_LUB_ROZBIEZNOSC',
        powody_v175=failure,
        dowod_wykonania_dokladnie_tych_bajtow=False)
    return d


def test_v175():
    import tempfile
    keys=('WEEX_EXECUTED_FILE','WEEX_EXPECTED_SHA256','WEEX_LAUNCH_SHA256',
          'WEEX_LAUNCHER_VERSION','WEEX_LAUNCH_TARGET_VERSION')
    before={k:os.environ.get(k) for k in keys}
    try:
        with tempfile.TemporaryDirectory() as td:
            p=Path(td)/'weex.py'
            p.write_text("VERSION = '"+VERSION+"'\n",encoding='utf-8')
            digest=hashlib.sha256(p.read_bytes()).hexdigest()
            os.environ.update(WEEX_EXECUTED_FILE=str(p),WEEX_EXPECTED_SHA256=digest,
                WEEX_LAUNCH_SHA256=digest,WEEX_LAUNCHER_VERSION='UNIWERSALNY_V2',
                WEEX_LAUNCH_TARGET_VERSION=VERSION)
            assert runtime_manifest_v175()['status_v175']=='ZGODNE_HASHE_WSKAZANEGO_PLIKU'
            os.environ['WEEX_EXPECTED_SHA256']='0'*64
            assert 'HASH_NIEZGODNY' in runtime_manifest_v175()['powody_v175']
            os.environ['WEEX_EXPECTED_SHA256']=digest
            os.environ['WEEX_LAUNCH_TARGET_VERSION']='V6.173'
            assert 'WERSJA_WYBRANA_PRZEZ_LAUNCHER_NIEZGODNA' in runtime_manifest_v175()['powody_v175']
    finally:
        for k,v in before.items():
            if v is None: os.environ.pop(k,None)
            else: os.environ[k]=v
    return 'PASS (zgodny plik, hash niezgodny, stara wersja launchera)'


def test_v173():
    import pandas as pd
    a = pd.DataFrame(columns=['event_id','status'])
    b = pd.DataFrame([{'event_id':'x', 'status':'oczekuje'}])
    out = b.reindex(columns=list(dict.fromkeys([*a.columns, *b.columns]))) if a.empty else pd.concat([a,b],ignore_index=True)
    assert len(out)==1 and out.iloc[0]['event_id']=='x'
    m = runtime_manifest_v173()
    assert m['version']==VERSION and 'status_hash_pliku_v173' in m
    return 'PASS (pusty dziennik bez concat, manifest launchera)'


def test_v172():
    import pandas as pd
    sample = pd.DataFrame([{'symbol':'X','side':'LONG','setup_type':'ABC','entry_type':'MARKET','powod':'brak'},
                           {'symbol':'X','side':'LONG','setup_type':'ABC','entry_type':'MARKET','powod':'brak'}])
    # Rownowaznosc nowej agregacji z dotychczasowa deduplikacja zachowujaca kolejnosc.
    grouped = {}
    for symbol, group in sample.groupby('symbol', sort=False):
        grouped[symbol] = '; '.join(dict.fromkeys(f'{a} {b} {c}: {r}' for a,b,c,r in zip(
            group['side'],group['setup_type'],group['entry_type'],group['powod'])))
    assert grouped['X'] == 'LONG ABC MARKET: brak'
    manifest = runtime_manifest_v172()
    assert manifest['version'] == VERSION and manifest['hash_nie_jest_dowodem_wykonania'] is True
    return 'PASS (groupby bez apply, manifest jawnie bez dowodu exec)'


def runtime_manifest_v149():
    d=runtime_manifest_v148()
    d['manifest_schema']=4
    d['version']=VERSION
    evidence=os.environ.get('WEEX_LAUNCH_SHA256','').strip().lower()
    actual=d.get('hash_wskazanego_pliku') or d.get('skrypt_sha256')
    expected=os.environ.get('WEEX_EXPECTED_SHA256','').strip().lower()
    d['status_weryfikacji_v149']=('ZGODNE_HASHE_PLIKU' if actual and expected and evidence and actual==expected==evidence
       else 'ROZBIEZNOSC_HASHY' if actual and ((expected and actual!=expected) or (evidence and actual!=evidence))
       else 'NIEZWERYFIKOWANE_BRAK_PELNEGO_LAUNCHERA')
    d['dowod_wykonania_bajtow']=False
    return d


def test_v149():
    a=status_przewagi_oos_v149('D', True)
    assert a['dopuszczenie_techniczne']=='TAK' and a['przewaga_oos_potwierdzona']=='NIE'
    assert a['ostrzezenie_ocena_d_v149'] and 'BEZ_POTWIERDZONEJ' in a['status_sygnalu_v149']
    assert status_przewagi_oos_v149('B',False)['dopuszczenie_techniczne']=='NIE'
    assert status_przewagi_oos_v149('A',True)['przewaga_oos_potwierdzona']=='NIE'
    return 'OK (ocena D i do_handlu nie sugeruja przewagi OOS)'



# V6.153: rzeczywisty replay tego samego zdarzenia na świecach 5m z dwoma kosztami.
def koszt_replay_ab_v153(rows, PP, simulator=None):
    """Bez modyfikacji aktywnego backtestu/bramki. Obydwa warianty maja identyczne OHLCV, zasady i funding.
    Brak pokrycia swiec = brak wyniku, nigdy sztucznie wyliczone R.
    """
    simulator = simulator or simulate
    out = []
    fund_cache = {}
    for row in rows:
        sym = row.get('symbol')
        P = PP.get(sym)
        base = {k:row.get(k) for k in ('snapshot_utc','symbol','typ_wersja','setup_type','side','entry_type','entry','sl','tp1','status','r')}
        if not P or '5m' not in P.get('arr', {}):
            out.append({**base,'pokrycie':'BRAK_5M'}); continue
        try:
            ts = int(pd.Timestamp(row['snapshot_utc']).timestamp()*1000)
            a5 = P['arr']['5m']
            if len(a5) == 0 or ts < int(a5[0,T]) or ts > int(a5[-1,T]):
                out.append({**base,'pokrycie':'BRAK_POKRYCIA_SNAPSHOT'}); continue
            if sym not in fund_cache: fund_cache[sym] = funding_points(P.get('raw'))
            fee, fund = P['fees'], fund_cache[sym]
            a = simulator(a5, ts, {**row, 'slip_bps':float(row.get('slip_bps') or HISTORICAL_SLIP_BPS)}, fee, fund)
            b = simulator(a5, ts, {**row, 'slip_bps':30.0}, fee, fund)
            record = {**base, 'pokrycie':'OK','model_bps':float(row.get('slip_bps') or HISTORICAL_SLIP_BPS), 'staly_bps':30.0,
                      'status_model':a.get('status'),'status_30':b.get('status'),
                      'filled_model':a.get('filled'),'filled_30':b.get('filled'),
                      'czesc_1r_model':a.get('czesc_1R',False),'czesc_1r_30':b.get('czesc_1R',False),
                      'fill_utc_model':a.get('fill_utc'),'fill_utc_30':b.get('fill_utc'),
                      'exit_utc_model':a.get('exit_utc'),'exit_utc_30':b.get('exit_utc'),
                      'r_model_replay':a.get('r'),'r_30_replay':b.get('r'),
                      'funding_R_model':a.get('funding_R'),'funding_R_30':b.get('funding_R'),
                      'luka_model':a.get('luka_danych'),'luka_30':b.get('luka_danych')}
            if a.get('status') in FINAL and b.get('status') in FINAL and not a.get('luka_danych') and not b.get('luka_danych'):
                record['delta_R_replay'] = round(float(a['r'])-float(b['r']),6)
                record['rozliczone_w_obu'] = True
            else:
                record['rozliczone_w_obu'] = False
            record['status_rozny'] = a.get('status') != b.get('status')
            out.append(record)
        except (ValueError, TypeError, KeyError, IndexError, AttributeError) as exc:
            out.append({**base,'pokrycie':'BLAD_REPLAY','szczegoly_bledu':str(exc)[:200]})
    return pd.DataFrame(out)


def test_koszt_replay_v153():
    # Oba warianty: te same sygnaly i ten sam simulator, tylko slip_bps sie rozni.
    x={'symbol':'X','snapshot_utc':'2026-10-29T10:00:00+00:00','side':'LONG','entry_type':'MARKET',
       'entry':100.,'sl':99.,'tp1':102.,'wait_h':2.,'setup_type':'TEST','slip_bps':10.,'r':-1.,'status':'SL'}
    ts=int(pd.Timestamp(x['snapshot_utc']).timestamp()*1000)
    candles=np.array([[100,101,98,99,10,1000,0,ts+i*INTERVAL_MS['5m']] for i in range(3)],dtype=float)
    calls=[]
    def fake(arr,t,r,fees,fund):
        calls.append(r['slip_bps']); return {'status':'SL','filled':True,'r':-1.-r['slip_bps']/100.,'exit_utc':'x'}
    p={'X':{'arr':{'5m':candles},'fees':(5.,2.),'raw':None}}
    d=koszt_replay_ab_v153([x],p,simulator=fake)
    assert calls==[10.,30.] and abs(float(d.iloc[0]['delta_R_replay'])-.2)<1e-9
    d2=koszt_replay_ab_v153([{**x,'snapshot_utc':'2026-10-01T10:00:00+00:00'}],p,simulator=fake)
    assert d2.iloc[0]['pokrycie']=='BRAK_POKRYCIA_SNAPSHOT'
    return 'PASS (wspolne swiece, dwa koszty, brak pokrycia bez wyliczen)'

def koszt_gate_replay_v155(bt_rows, replay_df, now_ms, od=None):
    """Badawcza gate_table na identycznych rozliczonych zdarzeniach 7d.
    Pozostale i nieporownywalne rekordy NIE sa dopisywane do zadnego wariantu.
    Benchmark control_R pozostaje oryginalny - pelny replay benchmarku nie jest dostepny.
    """
    required={'rozliczone_w_obu','r_model_replay','r_30_replay','status_model','status_30'}
    empty=pd.DataFrame(columns=['typ','side','wariant','status','transakcje','sredni_R','sredni_edge_R','t_edge','zdjecia','status_parowany','zmiana_decyzji_badawcza'])
    if replay_df is None or not len(replay_df) or not required.issubset(replay_df.columns):
        return empty, {'status':'BRAK_DANYCH_REPLAY','parowane':0}
    b=pd.DataFrame(bt_rows).reset_index(drop=True)
    r=replay_df.reset_index(drop=True)
    if len(b)!=len(r):
        return empty, {'status':'NIEZGODNA_LICZBA_REKORDOW','backtest':len(b),'replay':len(r)}
    # Indeks jest zachowany przez koszt_replay_ab_v153, ale dodatkowo sprawdzamy identyfikatory.
    key=('snapshot_utc','symbol','typ_wersja','side','entry_type')
    if any(k not in b or k not in r or not b[k].fillna('').astype(str).equals(r[k].fillna('').astype(str)) for k in key):
        return empty, {'status':'NIEZGODNA_KOLEJNOSC_LUB_IDENTYFIKATORY','backtest':len(b)}
    mask=r['rozliczone_w_obu'].fillna(False).astype(bool) & r['status_model'].isin(FINAL) & r['status_30'].isin(FINAL)
    for c in ('r_model_replay','r_30_replay'):
        mask &= pd.to_numeric(r[c],errors='coerce').notna()
    matched=b.loc[mask].copy()
    if matched.empty:
        return empty, {'status':'BRAK_PAROWANYCH_ROZLICZEN','parowane':0,'ogolem':len(b)}
    rr=r.loc[mask]
    tables=[]
    for variant,rc,sc in [('WOLUMEN_5_10_12_15','r_model_replay','status_model'),('STALE_30','r_30_replay','status_30')]:
        df=matched.copy()
        df['r']=pd.to_numeric(rr[rc],errors='coerce').to_numpy()
        df['status']=rr[sc].to_numpy()
        # control_R jest historycznym benchmarkiem w modelu bazowym. Jego niezalezne przeliczenie
        # wedlug 30 bps nie jest dostepne; edge jest tylko proxy, nie niezaleznie odtworzona bramka.
        if 'control_R' in df.columns:
            ctrl=pd.to_numeric(df['control_R'],errors='coerce')
            df['edge_R']=df['r']-ctrl
        elif 'edge_R' in df.columns:
            df['edge_R']=pd.NA
        if 'filled' in df.columns:
            fc='filled_model' if variant=='WOLUMEN_5_10_12_15' else 'filled_30'
            if fc in rr: df['filled']=rr[fc].to_numpy()
        g=gate_table(df,now_ms,od)
        g['wariant']=variant
        tables.append(g)
    combined=pd.concat(tables,ignore_index=True)
    wide=combined.pivot(index=['typ','side'],columns='wariant',values='status')
    changed={idx:(row.get('WOLUMEN_5_10_12_15')!=row.get('STALE_30')) for idx,row in wide.iterrows()}
    combined['zmiana_decyzji_badawcza']=[changed.get((t,sd),False) for t,sd in zip(combined.typ,combined.side)]
    combined['status_parowany']='TYLKO_POROWNYWALNE_ROZLICZENIA_7D'
    return combined, {'status':'BADAWCZY_SUBSET_7D','parowane':int(mask.sum()),'ogolem':int(len(b)),
        'odrzucone_lub_otwarte':int((~mask).sum()), 'unikalne_zdjecia':int(matched.snapshot_utc.nunique()),
        'zmienione_grupy_badawcze':int(sum(changed.values())),
        'benchmark':'CONTROL_R_WSPOLNY_Z_MODELU_BAZOWEGO_NIE_REPLAYOWANY',
        'ograniczenia':['tylko rozliczone w obu; selekcja moze miec bias','nie obejmuje calego archiwum gate',
                        'control_R nie jest ponownie symulowany','nie zmienia gate.csv ani do_handlu']}


def test_koszt_gate_v155():
    ts='2026-10-29T10:00:00+00:00'
    b=[{'snapshot_utc':ts,'symbol':'X','typ_wersja':'PULLBACK.v4.M','setup_type':'PULLBACK','side':'LONG',
        'entry_type':'MARKET','entry':100.,'status':'SL','r':-1.,'control_R':-0.5,'edge_R':-0.5,
        'filled':True,'filtr':'','zone_ref':100.}]
    r=pd.DataFrame([{'snapshot_utc':ts,'symbol':'X','typ_wersja':'PULLBACK.v4.M','side':'LONG','entry_type':'MARKET',
        'rozliczone_w_obu':True,'r_model_replay':-1.,'r_30_replay':-1.2,
        'status_model':'SL','status_30':'SL','filled_model':True,'filled_30':True}])
    d,meta=koszt_gate_replay_v155(b,r,int(pd.Timestamp(ts).timestamp()*1000))
    assert len(d)>0 and meta['parowane']==1 and set(d.wariant)=={'STALE_30','WOLUMEN_5_10_12_15'}
    assert meta['benchmark'].startswith('CONTROL_R_')
    r.loc[0,'rozliczone_w_obu']=False
    _,meta2=koszt_gate_replay_v155(b,r,int(pd.Timestamp(ts).timestamp()*1000))
    assert meta2['parowane']==0
    return 'PASS (ta sama populacja, obie metody, brak sztucznego rozliczenia)'


# V6.156 - niezalezne KANDYDATURY BADAWCZE ABC, bez zlecen/handlu.
def elliott_wlasne_abc_v156(summary_rows, snapshot_utc):
    """Tworzy hipotezy cenowe z wykrytych ABC, nigdy produkcyjne setupy.

    SL za potwierdzonym C z buforem 0.2 ATR, cele 1.5/2.5 R sa
    PROJEKCJAMI, a nie potwierdzonymi oporami/wsparciami ani prognoza.
    ATR odtwarzamy wylacznie z pomiaru w chwili snapshotu; brak => bez cen.
    """
    out=[]
    for row in summary_rows:
        if row.get('elliott_abc_status') != 'ABC_POTENCJALNIE_ZAKONCZONE':
            continue
        side=row.get('elliott_abc_kierunek')
        if side not in ('LONG','SHORT'):
            continue
        rec={'symbol':row.get('symbol'), 'side':side, 'setup_type':'ELLIOTT_ABC_RESEARCH.v1',
             'entry_type':'MARKET', 'snapshot_utc':snapshot_utc,
             'elliott_zrodlo_wejscia':'WLASNA_HIPOTEZA_ABC',
             'elliott_rola':'SAMODZIELNY_KANDYDAT_BADAWCZY',
             'ranking_kategoria':'ELLIOTT', 'ranking_status':'OBSERWACJA',
             'ranking_charakter':'BADAWCZY_NIE_JEST_SYGNALEM',
             'dopuszczenie_techniczne':'NIE', 'do_handlu':False,
             'przewaga_oos_potwierdzona':'NIE', 'ocena':'BRAK_WALIDACJI',
             'ranking_jakosc':'BRAK_WALIDACJI', 'status_setupu':'OBSERWACJA',
             'ranking_wykonalnosc':'NIE_DOPUSZCZONY',
             'ranking_powod':'NIEPRZETESTOWANA_STRATEGIA_ABC',
             'elliott_abc_status':row.get('elliott_abc_status'),
             'elliott_abc_swiezosc':row.get('elliott_abc_swiezosc'),
             'elliott_abc_zgodnosc_4h':row.get('elliott_abc_zgodnosc_4h'),
             'elliott_abc_wiek_h':row.get('elliott_abc_wiek_h'),
             'elliott_abc_od_c_atr':row.get('elliott_abc_od_c_atr'),
             'elliott_abc_c':row.get('elliott_abc_c'),
             'elliott_atr_1h_v157':row.get('elliott_atr_1h_v157'),
             'elliott_abc_c_utc_ms':row.get('elliott_abc_c_utc_ms'),
             'elliott_abc_uniewaznienie':row.get('elliott_abc_uniewaznienie'),
             'tradable':row.get('tradable'), 'not_tradable_reason':row.get('not_tradable_reason'),
             'asset_type':row.get('asset_type'),
             'model_celow_v156':'HIPOTEZA_TP1_1_5R_TP2_2_5R',
             'warunki':'Tylko obserwacja; wymagane swieze ABC, zgodnosc 4h i walidacja OOS',
             'price':row.get('live_price'), 'entry':None, 'sl':None,
             'tp1':None, 'tp2':None, 'rr_tp1_net':None,
             'risk_pct':None, 'elliott_kwalifikacja_v158':'NIEOCENIONA',
             'elliott_geometria_sl_v158':'NIEOCENIONA',
             'elliott_okazja_id_v158':str(row.get('symbol'))+'|'+side+'|'+str(row.get('elliott_abc_c_utc_ms') or snapshot_utc)}
        def finite(v):
            try:
                n=float(v)
                return n if np.isfinite(n) else None
            except (TypeError,ValueError):
                return None
        price=finite(row.get('live_price'))
        c=finite(row.get('elliott_abc_c'))
        atr_direct=finite(row.get('elliott_atr_1h_v157'))
        dist=finite(row.get('elliott_abc_od_c_atr'))
        # Zrodlo podstawowe: niezaokraglony pomiar ATR 1h z analizy snapshotu.
        # Starsze dane bez ATR mozna odtworzyc tylko gdy dystans jest wiarygodny.
        atr_proxy=(atr_direct if atr_direct is not None and atr_direct > 0 else
                   abs(price-c)/dist if price is not None and c is not None and
                   dist is not None and dist > 0 else None)
        if price is None or c is None or atr_proxy is None or atr_proxy <= 0 or price <= 0:
            rec['elliott_status_kandydata_v156']='BRAK_WIARYGODNEGO_ATR_LUB_CENY'
        else:
            sign=1 if side=='LONG' else -1
            sl=c-sign*0.2*atr_proxy
            risk=sign*(price-sl)
            if (not np.isfinite(atr_proxy) or not np.isfinite(sl) or sl <= 0 or risk <= 0
                    or (side=='LONG' and (not sl < c < price))
                    or (side=='SHORT' and (not price < c < sl))):
                rec['elliott_status_kandydata_v156']='NIEPOPRAWNA_GEOMETRIA'
                rec['elliott_geometria_sl_v158']='ODRZUCONA'
            else:
                rec.update(entry=price, sl=round(sl,10), tp1=round(price+sign*1.5*risk,10),
                           tp2=round(price+sign*2.5*risk,10),
                           risk_pct=round(100*risk/price,3), rr_tp1_net=None)
                rec['elliott_geometria_sl_v158']='POPRAWNA'
                rec['elliott_rr_brutto_v156']=1.5
                rec['elliott_status_kandydata_v156']='CENY_BADAWCZE'
        # Nie zakladamy swiezosci: sprawdzamy wiek C i dystans od C w ATR.
        age=finite(row.get('elliott_abc_wiek_h'))
        if age is None:
            try:
                c_ms=finite(row.get('elliott_abc_c_utc_ms'))
                now_ms_=pd.Timestamp(snapshot_utc).timestamp()*1000
                age=max(0.,(now_ms_-c_ms)/HOUR_MS) if c_ms is not None else None
            except (ValueError, TypeError, OverflowError):
                age=None
        distance=abs(price-c)/atr_proxy if price is not None and c is not None and atr_proxy and atr_proxy>0 else None
        if age is None or distance is None:
            freshness='NIEZWERYFIKOWANY'
        elif age<=12 and distance<=1.5:
            freshness='SWIEZY'
        elif distance>2.5:
            freshness='ODDALONY'
        else:
            freshness='DO_OBSERWACJI'
        rec['elliott_abc_swiezosc']=freshness
        rec['elliott_abc_wiek_h']=round(age,2) if age is not None else None
        rec['elliott_abc_od_c_atr']=round(distance,3) if distance is not None else None
        if freshness!='SWIEZY':
            rec['elliott_status_kandydata_v156']=freshness+'_'+str(rec['elliott_status_kandydata_v156'])
        if row.get('elliott_abc_zgodnosc_4h')!='ZGODNE':
            rec['elliott_status_kandydata_v156']='BRAK_ZGODNOSCI_4H_'+str(rec['elliott_status_kandydata_v156'])
        if not row.get('tradable'):
            rec['elliott_status_kandydata_v156']='PROBLEM_DANYCH_'+str(rec['elliott_status_kandydata_v156'])
        rec['elliott_kwalifikacja_v158'] = ('SWIEZY_ZGODNY_4H_CENY_POPRAWNE'
            if rec['elliott_geometria_sl_v158']=='POPRAWNA' and freshness=='SWIEZY'
            and row.get('elliott_abc_zgodnosc_4h')=='ZGODNE' and bool(row.get('tradable'))
            else 'TYLKO_OBSERWACJA_BRAK_KOMPLETU_WARUNKOW')
        rec['elliott_status_strategii_bazowej_v158']='NIE_DOTYCZY_WLASNE_ABC'
        rec['ranking_setup_id']=str(rec['symbol'])+' | ELLIOTT_ABC_RESEARCH.v1 | '+side+' | MARKET'
        out.append(rec)
    return out


def test_elliott_wlasne_v156():
    row={'symbol':'TEST','elliott_abc_status':'ABC_POTENCJALNIE_ZAKONCZONE',
         'elliott_abc_kierunek':'LONG','elliott_abc_c':90.,'live_price':92.,
         'elliott_abc_od_c_atr':1.,'elliott_abc_swiezosc':'SWIEZY',
         'elliott_abc_wiek_h':2.,'elliott_atr_1h_v157':2.,
         'elliott_abc_zgodnosc_4h':'ZGODNE','tradable':True}
    a=elliott_wlasne_abc_v156([row],'test')
    assert len(a)==1 and a[0]['entry']==92 and a[0]['sl']<90 and a[0]['tp1']>92
    assert a[0]['do_handlu'] is False and a[0]['ranking_status']=='OBSERWACJA'
    b=elliott_wlasne_abc_v156([{**row,'elliott_abc_od_c_atr':None}],'test')
    assert b[0]['entry'] is not None and b[0]['sl'] is not None  # bez dystansu, ale z pomiarem ATR 1h
    missing=elliott_wlasne_abc_v156([{**row,'elliott_atr_1h_v157':None,'elliott_abc_od_c_atr':None}],'test')
    assert missing[0]['entry'] is None and missing[0]['sl'] is None
    short=elliott_wlasne_abc_v156([{**row,'elliott_abc_kierunek':'SHORT',
            'elliott_abc_c':94.,'elliott_abc_od_c_atr':1.}],'test')
    assert short[0]['sl']>94 and short[0]['tp1']<92
    assert a[0]['elliott_abc_swiezosc']=='SWIEZY'
    stale=elliott_wlasne_abc_v156([{**row,'elliott_abc_wiek_h':30.,'elliott_abc_swiezosc':None}],'test')
    assert stale[0]['elliott_abc_swiezosc']!='SWIEZY'
    return 'PASS (LONG, SHORT, ATR bez dystansu, brak ATR, wiek C, OBSERWACJA)'


def test_elliott_v158():
    base={'symbol':'TEST', 'elliott_abc_status':'ABC_POTENCJALNIE_ZAKONCZONE',
          'elliott_abc_kierunek':'LONG', 'elliott_abc_c':90., 'live_price':92.,
          'elliott_atr_1h_v157':2., 'elliott_abc_wiek_h':2.,
          'elliott_abc_zgodnosc_4h':'ZGODNE', 'tradable':True}
    good=elliott_wlasne_abc_v156([base],'2026-10-29T15:00:00Z')[0]
    assert good['elliott_geometria_sl_v158']=='POPRAWNA'
    assert good['elliott_kwalifikacja_v158']=='SWIEZY_ZGODNY_4H_CENY_POPRAWNE'
    assert good['do_handlu'] is False
    bad=elliott_wlasne_abc_v156([{**base,'elliott_abc_c':93.}],'2026-10-29T15:00:00Z')[0]
    assert bad['entry'] is None and bad['elliott_geometria_sl_v158']=='ODRZUCONA'
    assert bad['elliott_kwalifikacja_v158']!='SWIEZY_ZGODNY_4H_CENY_POPRAWNE'
    return 'PASS (geometria LONG, odrzucenie zlego SL, oddzielna kwalifikacja, obserwacja)'



# V6.159: odrebny test HISTORYCZNY samodzielnego ABC; tylko badanie.
# Siatka co 12h celowo ogranicza koszty CPU: nie jest pelnym OOS ani walk-forward
# szkoleniem parametrow. Nie uzywa biezacej ceny ani biezacego orderbooka.
def elliott_koszt_r_v161(entry, sl, slip_bps, fees_bps):
    """Hipotetyczny round-trip taker/taker w R, wszystkie koszty w bps.

    Nie zastępuje rozliczenia simulate (różne nogi i fill); brak geometrii = None.
    """
    try:
        entry, sl, slip = float(entry), float(sl), float(slip_bps)
        taker = float(fees_bps[0])
        risk_frac = abs(entry-sl)/entry
        if not all(np.isfinite(x) for x in (entry, sl, slip, taker)) or entry <= 0 or risk_frac <= 0 or slip < 0 or taker < 0:
            return None
        return round((2.0*slip + 2.0*taker)/10000.0/risk_frac, 5)
    except (TypeError, ValueError, IndexError, ZeroDivisionError):
        return None


def elliott_statusy_v161(rows):
    """Rozłączne populacje dla kontroli liczebności i sum raportu."""
    counts={'kandydatury':len(rows), 'rozliczone':0, 'otwarte':0,
            'oczekujace':0, 'inne_nierozliczone':0, 'brak_danych':0}
    for r in rows:
        status=str(r.get('status','')).lower()
        if r.get('luka_danych') or status=='blad_analizy':
            counts['brak_danych']+=1
        elif r.get('status') in FINAL and r.get('r_netto') is not None:
            counts['rozliczone']+=1
        elif status in ('otwarty','open'):
            counts['otwarte']+=1
        elif status in ('oczekuje','pending'):
            counts['oczekujace']+=1
        else:
            counts['inne_nierozliczone']+=1
    assert sum(v for k,v in counts.items() if k!='kandydatury')==counts['kandydatury']
    return counts


def elliott_backtest_abc_v159(PP, now_ms, lookback_h=168, step_h=12):
    rows, seen = [], set()
    step = int(step_h * HOUR_MS)
    last = (int(now_ms) // step) * step
    snaps = list(range(last - int(lookback_h * HOUR_MS), last + 1, step))
    for ts in snaps:
        for sym, P in PP.items():
            A = view(P, ts)
            if not enough(A):
                continue
            try:
                price = float(A['5m'][-1, C])
                if not np.isfinite(price) or price <= 0:
                    continue
                f = market_features(A, price, ts, None, light=True)[0]
                if not f.get('atr1') or not np.isfinite(float(f['atr1'])):
                    continue
                f.update(elliott_struktura(A))
                hist_row = dict(f)
                hist_row.update(symbol=sym, live_price=price, tradable=True,
                                asset_type=P.get('asset_type'),
                                elliott_atr_1h_v157=f['atr1'])
                candidates = elliott_wlasne_abc_v156([hist_row], snap_iso(ts))
                for rec in candidates:
                    # Jeden zarejestrowany sygnal na potwierdzony punkt C, bez wielokrotnego
                    # naliczania tej samej okazji w kolejnych snapshotach.
                    key = (sym, rec['side'], rec.get('elliott_abc_c_utc_ms'))
                    if key in seen:
                        continue
                    # Deduplikacja dopiero po pojawieniu sie swiezej, geometrycznie
                    # prawidlowej hipotezy: stare/niekompletne obserwacje jej nie zuzywaja.
                    if rec.get('elliott_kwalifikacja_v158') != 'SWIEZY_ZGODNY_4H_CENY_POPRAWNE':
                        continue
                    seen.add(key)
                    r = dict(rec)
                    r.update(wait_h=0, slip_bps=historical_slip_for(P, ts,
                              float(A['1h'][-24:, QV].sum()) if len(A['1h']) >= 24 else None))
                    sim = simulate(P['arr']['5m'], ts, r, P['fees'], funding_points(P.get('raw')))
                    koszt_r = elliott_koszt_r_v161(r['entry'], r['sl'], r['slip_bps'], P['fees'])
                    rows.append({
                        'snapshot_utc':snap_iso(ts), 'symbol':sym, 'side':r['side'],
                        'asset_type':P.get('asset_type','NIEZNANY'),
                        'rynek_v160':('CRYPTO' if P.get('asset_type')=='COIN' else 'TRADFI'),
                        'koszt_r_est_v160':koszt_r,
                        'ekonomika_v160':('NIEZNANY_KOSZT' if koszt_r is None else
                                           'WYSOKI_KOSZT' if koszt_r > 0.5 else 'OK_BADAWCZO'),
                        'elliott_abc_c_utc_ms':r.get('elliott_abc_c_utc_ms'),
                        'entry_snapshot':r['entry'], 'sl':r['sl'], 'tp1':r['tp1'], 'tp2_badawcze':r['tp2'],
                        'slip_bps':r['slip_bps'], 'status':sim['status'],
                        'filled':sim.get('filled', False), 'r_netto':sim.get('r'),
                        'czesc_1R':sim.get('czesc_1R', False),
                        'fill_utc':sim.get('fill_utc'), 'exit_utc':sim.get('exit_utc'),
                        'luka_danych':sim.get('luka_danych',False),
                        'uwaga':'TP2 to projekcja informacyjna; silnik rozlicza TP1, SL i zasady +1R'
                    })
            except (KeyError, ValueError, TypeError, ArithmeticError, IndexError) as ex:
                rows.append({'snapshot_utc':snap_iso(ts), 'symbol':sym,
                             'status':'BLAD_ANALIZY', 'uwaga':type(ex).__name__+': '+str(ex)[:120]})
    return rows, {'model':'ABC_RESEARCH.v1', 'snapshot_step_h':step_h,
                  'lookback_h':lookback_h, 'snapshots':len(snaps),
                  'universe':'wszystkie instrumenty PP z pelnym oknem',
                  'unikalnosc':'symbol+kierunek+czas punktu C',
                  'rynek':'cena snapshotu do budowy hipotezy; MARKET wykonanie nastepna swieca',
                  'wyjscie':'silnik simulate: SL, TP1=1.5R, czesc +1R, BE/trailing, timeout, prowizje, slippage, funding',
                  'tp2':'2.5R raportowane, NIE SYMULOWANE jako drugi take profit',
                  'status':'BADAWCZY_BEZ_DOPUSZCZENIA',
                  'ograniczenia':['rzadka siatka 12h pomija sygnaly pomiedzy snapshotami',
                                 'brak rekonstrukcji historycznego spreadu/orderbooka',
                                 'historyczny test na dostepnych swiecach, nie niezalezny OOS',
                                 'brak zarzadzania portfelem i korelacji']}


def elliott_podsumowanie_v160(rows, step_h):
    """Osobne populacje; tylko zakończone bez luki, brak optymalizacji siatki."""
    out=[]
    for rynek in ('CRYPTO','TRADFI','RAZEM'):
        subset=[r for r in rows if rynek=='RAZEM' or r.get('rynek_v160')==rynek]
        resolved=[r for r in subset if r.get('status') in FINAL and not r.get('luka_danych') and r.get('r_netto') is not None]
        values=[float(r['r_netto']) for r in resolved]
        costly=[r for r in resolved if r.get('ekonomika_v160')=='WYSOKI_KOSZT']
        cheap=[r for r in resolved if r.get('ekonomika_v160')=='OK_BADAWCZO']
        for group, group_rows in [('WSZYSTKIE',resolved),('KOSZT_DO_0_5R',cheap),('KOSZT_PONAD_0_5R',costly)]:
            arr=[float(r['r_netto']) for r in group_rows]
            status_counts = elliott_statusy_v161(subset) if group=='WSZYSTKIE' else {}
            out.append({'krok_h':step_h,'rynek':rynek,'grupa':group,
                        'kandydatury':len(subset) if group=='WSZYSTKIE' else None,
                        'otwarte':status_counts.get('otwarte'), 'oczekujace':status_counts.get('oczekujace'),
                        'inne_nierozliczone':status_counts.get('inne_nierozliczone'),
                        'brak_danych':status_counts.get('brak_danych'),
                        'rozliczone':len(arr),'sredni_R':sum(arr)/len(arr) if arr else None,
                        'suma_R':sum(arr) if arr else None,
                        'odsetek_zysku':sum(v>0 for v in arr)/len(arr) if arr else None,
                        'status':'BADAWCZY_NIE_OOS'})
    return out


def test_v160():
    sample=[{'rynek_v160':'CRYPTO','status':'SL','r_netto':-1.,'ekonomika_v160':'WYSOKI_KOSZT'},
            {'rynek_v160':'TRADFI','status':'TP1','r_netto':1.2,'ekonomika_v160':'OK_BADAWCZO'}]
    o=elliott_podsumowanie_v160(sample,12)
    assert len(o)==9 and next(r for r in o if r['rynek']=='CRYPTO' and r['grupa']=='WSZYSTKIE')['suma_R']==-1.
    assert next(r for r in o if r['rynek']=='RAZEM' and r['grupa']=='WSZYSTKIE')['rozliczone']==2
    return 'PASS (rynki, koszt/R, podzial i agregacja)'


def test_elliott_bt_v159():
    # bez sieci: brak instrumentow, deterministyczne metadane
    rows, meta = elliott_backtest_abc_v159({}, 168*HOUR_MS, 24, 12)
    assert rows == [] and meta['snapshots'] == 3 and meta['status'] == 'BADAWCZY_BEZ_DOPUSZCZENIA'
    assert meta['tp2'].startswith('2.5R')
    return 'PASS'

def test_v161():
    # 10 bps poślizgu + 6 bps prowizji na każdą z dwóch nóg,
    # ryzyko ceny 1% => 32 bps / 100 bps = 0.32 R.
    assert elliott_koszt_r_v161(100, 99, 10, (6,2)) == 0.32
    assert elliott_koszt_r_v161(100, 100, 10, (6,2)) is None
    rows=[{'rynek_v160':'CRYPTO','status':'SL','r_netto':-1.,'ekonomika_v160':'WYSOKI_KOSZT'},
          {'rynek_v160':'CRYPTO','status':'otwarty','r_netto':None},
          {'rynek_v160':'CRYPTO','status':'oczekuje','r_netto':None},
          {'rynek_v160':'TRADFI','status':'TP1','r_netto':1.2,'ekonomika_v160':'OK_BADAWCZO'},
          {'rynek_v160':'TRADFI','status':'BLAD_ANALIZY'}]
    c=elliott_statusy_v161(rows)
    assert (c['kandydatury'], c['rozliczone'], c['otwarte'], c['oczekujace'], c['brak_danych']) == (5,2,1,1,1)
    out=elliott_podsumowanie_v160(rows,12)
    total=next(x for x in out if x['rynek']=='RAZEM' and x['grupa']=='WSZYSTKIE')
    assert total['rozliczone']==2 and abs(total['suma_R']-0.2)<1e-10
    assert total['kandydatury']==sum(total[k] for k in ('rozliczone','otwarte','oczekujace','inne_nierozliczone','brak_danych'))
    return 'PASS (bps->R, brak SL, stany rozlaczne, sumy)'


# V6.162: unikaty miedzy siatkami i progi testowane BEZ modyfikacji handlu.
V162_PROGI = (0.25, 0.35, 0.50, 0.75)

def abc_v162_czas_ms(value):
    if value is None or str(value).strip() in ('', 'None', 'nan'):
        return None
    try:
        if isinstance(value, (int, float)):
            return int(value)
        return int(pd.Timestamp(value).timestamp()*1000)
    except (TypeError, ValueError, OverflowError):
        return None


def abc_v162_klucz(r):
    c = abc_v162_czas_ms(r.get('elliott_abc_c_utc_ms'))
    if c is None:
        return None  # bez C nie wolno deduplikowac roznych okazji
    return (str(r.get('symbol')), str(r.get('side')), c)


def abc_v162_unikaty(rows):
    # Kanoniczny obserwowany sygnal = najwczesniejszy snapshot z dostepnych siatek.
    # Pozostale obserwacje tej samej formacji nie sa nowymi niezaleznymi probami.
    ordered=sorted(rows, key=lambda r:(abc_v162_czas_ms(r.get('snapshot_utc')) or 0,
                                      float(r.get('krok_h') or 999), str(r.get('symbol'))))
    out=[]; seen=set()
    for r in ordered:
        key=abc_v162_klucz(r)
        if key is None:
            # brak punktu C: nie udawac unikalnego eventu
            continue
        if key in seen: continue
        seen.add(key)
        out.append({**r, 'unikalny_abc_id_v162':'|'.join(map(str,key)),
                    'kanoniczny_krok_h_v162':r.get('krok_h')})
    return out


def abc_v162_rozliczone(rows):
    out=[]
    for r in rows:
        if r.get('status') not in FINAL or r.get('luka_danych'):
            continue
        try:
            cost=float(r.get('koszt_r_est_v160'))
            result=float(r.get('r_netto'))
            if np.isfinite(cost) and cost>=0 and np.isfinite(result):
                out.append((r, cost, result))
        except (ValueError, TypeError):
            pass
    return out


def abc_v162_statystyki(rows, prog):
    eligible=[(r,c,y) for r,c,y in abc_v162_rozliczone(rows) if c<=prog]
    ys=[y for _,_,y in eligible]
    costs=[c for _,c,_ in eligible]
    # Gross-proxy to tylko mechaniczne dodanie uproszczonego kosztu, NIE backtest brutto.
    proxy=[y+c for (_,c,y) in eligible]
    return {'prog_r':prog,'liczba':len(ys),'sredni_R_netto':sum(ys)/len(ys) if ys else None,
            'suma_R_netto':sum(ys) if ys else None,
            'sredni_koszt_R_est':sum(costs)/len(costs) if costs else None,
            'sredni_R_plus_koszt_proxy':sum(proxy)/len(proxy) if proxy else None,
            'zyskowne':sum(x>0 for x in ys),
            'dni_sygnalow':len({str(r.get('snapshot_utc',''))[:10] for r,_,_ in eligible})}


def abc_v162_analiza(all_rows):
    unique=abc_v162_unikaty(all_rows)
    # Jedna populacja niezaleznych okazji. Nie wybierac najlepszego kroku 3/6/12h.
    evaluable=abc_v162_rozliczone(unique)
    details=[]
    for market in ('CRYPTO','TRADFI','RAZEM'):
        subset=[r for r in unique if market=='RAZEM' or r.get('rynek_v160')==market]
        for threshold in V162_PROGI:
            stats=abc_v162_statystyki(subset,threshold)
            stats.update(rynek=market, populacja='UNIKALNE_ABC',
                         wszystkie_unikalne=len(subset),
                         wszystkie_rozliczone_z_kosztem=len(abc_v162_rozliczone(subset)),
                         status='BADAWCZY_NIE_OOS')
            details.append(stats)
    # Chronologiczny, z gory okreslony split 60/40 wedlug czasu sygnalu.
    # Exit z czesci treningowej musi byc potwierdzony przed pierwszym sygnalem testowym.
    dated=sorted([r for r in unique if abc_v162_czas_ms(r.get('snapshot_utc')) is not None],
                 key=lambda r:abc_v162_czas_ms(r.get('snapshot_utc')))
    cut=abc_v162_czas_ms(dated[max(0,min(len(dated)-1,int(len(dated)*0.60)))]['snapshot_utc']) if len(dated)>=2 else None
    train=[]; test=[]; skipped_overlap=0
    for r in dated:
        ts=abc_v162_czas_ms(r.get('snapshot_utc'))
        if ts<cut:
            ex=abc_v162_czas_ms(r.get('exit_utc'))
            if ex is not None and ex<cut: train.append(r)
            else: skipped_overlap+=1
        else: test.append(r)
    wf=[]
    for market in ('CRYPTO','TRADFI','RAZEM'):
        a=[r for r in train if market=='RAZEM' or r.get('rynek_v160')==market]
        b=[r for r in test if market=='RAZEM' or r.get('rynek_v160')==market]
        scored=[abc_v162_statystyki(a,t) for t in V162_PROGI]
        # Wymagaj minimum 8 rozliczen, 2 dni, zanim wybierzesz prog. Brak podstaw => brak selekcji.
        eligible=[v for v in scored if v['liczba']>=8 and v['dni_sygnalow']>=2]
        best=max(eligible,key=lambda v:(v['sredni_R_netto'],-v['prog_r'])) if eligible else None
        for v in scored:
            wf.append({'rynek':market,'zbior':'TRAIN','wybrany_prog_r':best['prog_r'] if best else None,
                       'podzial_utc':snap_iso(cut) if cut else None,'transakcje_z_pelnym_exitem_przed_testem':len(abc_v162_rozliczone(a)),
                       'odrzucone_train_exit_po_podziale':skipped_overlap,
                       'status':'BADAWCZY' if best else 'BRAK_DANYCH_DO_WYBORU',**v})
        if best:
            wf.append({'rynek':market,'zbior':'TEST_OOS_WEWNETRZNY','wybrany_prog_r':best['prog_r'],
                       'podzial_utc':snap_iso(cut), 'transakcje_z_pelnym_exitem_przed_testem':len(abc_v162_rozliczone(a)),
                       'odrzucone_train_exit_po_podziale':skipped_overlap,
                       'status':'KRÓTKI_TEST_NIEPOTWIERDZA_PRZEWAGI',**abc_v162_statystyki(b,best['prog_r'])})
        else:
            wf.append({'rynek':market,'zbior':'TEST_OOS_WEWNETRZNY','wybrany_prog_r':None,
                       'podzial_utc':snap_iso(cut) if cut else None,
                       'status':'NIE_OCENIANO_BRAK_PROGU_W_TRAIN','prog_r':None,'liczba':0})
    meta={'wersja':'V6.162','progi_r':V162_PROGI,'unikalne_abc':len(unique),
          'brak_punktu_c_pominiete':sum(abc_v162_klucz(r) is None for r in all_rows),
          'liczba_obserwacji_w_siatkach':len(all_rows),
          'podzial_czasowy_utc':snap_iso(cut) if cut else None,
          'train_exit_po_cut_pominiete':skipped_overlap,
          'zasada':'unikaty: symbol+kierunek+punkt C; najwczesniejszy wykryty snapshot',
          'walk_forward':'60/40 chronologicznie, wybor progu tylko na rozliczonych przed testem; minimum 8 i 2 dni',
          'limitations':['Siedem dni danych to za malo do wiarygodnej walidacji OOS',
                         'Siatki 3/6/12h skorelowane; unikaty nie dowodza niezaleznosci rynkowej',
                         'Cost/R to aproksymacja taker/taker, a R+cost nie jest rzeczywistym R brutto',
                         'Kanoniczny najwczesniejszy snapshot moze nie byc jedynym wykonalnym wariantem',
                         'Brak historycznego orderbooka; bez zmiany handlu, gate i PROBA']}
    return unique,details,wf,meta


# V6.163: paired chronological no-filter vs train-selected cost/R threshold.
# Analytical only: no modifications to live signals, gate, PROBA or simulate().
def abc_v163_porownanie(all_rows):
    unique = abc_v162_unikaty(all_rows)
    dated = sorted((r for r in unique if abc_v162_czas_ms(r.get('snapshot_utc')) is not None),
                   key=lambda r:abc_v162_czas_ms(r.get('snapshot_utc')))
    if len(dated)<2:
        return [], {'wersja':'V6.163','status':'BRAK_DATOWANEJ_PROBY', 'unikalne':len(unique)}
    cut=abc_v162_czas_ms(dated[max(0,min(len(dated)-1,int(len(dated)*0.60)))]['snapshot_utc'])
    train=[]; test=[]; overlap=[]
    for r in dated:
        ts=abc_v162_czas_ms(r.get('snapshot_utc'))
        if ts<cut:
            ex=abc_v162_czas_ms(r.get('exit_utc'))
            if ex is not None and ex<cut: train.append(r)
            else: overlap.append(r)
        else: test.append(r)
    rows=[]
    for market in ('CRYPTO','TRADFI','RAZEM'):
        subset=lambda rr:[r for r in rr if market=='RAZEM' or r.get('rynek_v160')==market]
        a,b=subset(train),subset(test)
        eligible=[abc_v162_statystyki(a,t) for t in V162_PROGI]
        eligible=[s for s in eligible if s['liczba']>=8 and s['dni_sygnalow']>=2]
        best=max(eligible,key=lambda s:(s['sredni_R_netto'],-s['prog_r'])) if eligible else None
        # Fair comparison: both strategies use the exact same settled, cost-known events.
        for name,pop in (('TRAIN',a),('TEST_CHRONOLOGICZNY',b)):
            settled=abc_v162_rozliczone(pop)
            baseline=[(r,c,y) for r,c,y in settled]
            selected=[(r,c,y) for r,c,y in settled if best and c<=best['prog_r']]
            rejected=[(r,c,y) for r,c,y in settled if best and c>best['prog_r']]
            def stats(triples):
                yy=[t[2] for t in triples]
                return (len(yy),sum(yy) if yy else 0.0,sum(yy)/len(yy) if yy else None,
                        sum(y>0 for y in yy))
            n0,sum0,avg0,win0=stats(baseline)
            n1,sum1,avg1,win1=stats(selected)
            nr,sumr,avgr,winr=stats(rejected)
            determined=best is not None
            rows.append(dict(rynek=market,zbior=name,podzial_utc=snap_iso(cut),
                status='BADAWCZY_WYNIK_UJEMNY_NIE_POTWIERDZA_PRZEWAGI' if determined else 'BRAK_PROGU_Z_TRAIN_NIE_OCENIANO',
                prog_wybrany_train_r=best['prog_r'] if determined else None,
                wszystkie_sygnaly=len(pop),rozliczone_z_kosztem=n0,
                bez_filtra_n=n0,bez_filtra_suma_R=sum0,bez_filtra_sredni_R=avg0,bez_filtra_zyskowne=win0,
                z_filtrem_n=n1 if determined else None,
                z_filtrem_suma_R=sum1 if determined else None,
                z_filtrem_sredni_R=avg1 if determined else None,
                z_filtrem_zyskowne=win1 if determined else None,
                odrzucone_przez_filtr_n=nr if determined else None,
                odrzucone_suma_R=sumr if determined else None,
                odrzucone_sredni_R=avgr if determined else None,
                zmiana_sredniego_R_vs_bez_filtra=(avg1-avg0) if determined and avg1 is not None and avg0 is not None else None,
                zmiana_sumy_R_vs_bez_filtra=sum1-sum0 if determined else None,
                odsetek_zaakceptowanych=n1/n0 if determined and n0 else None,
                train_wylaczone_nierozliczone_przed_cut=len(subset(overlap)),
                uwaga='Rozna liczba transakcji po filtrze: srednich nie interpretowac jako zwrotu portfela; suma R bez normalizacji ekspozycji.'))
            if determined:
                assert n0==n1+nr
                assert abs(sum0-sum1-sumr)<1e-7
    meta={'wersja':'V6.163','status':'TYLKO_BADAWCZY',
          'unikalne_okazje':len(unique),'podzial_utc':snap_iso(cut),
          'metoda':'Jeden podzial 60/40 czasu sygnalow; prog trenowany tylko na wyjsciach sprzed cut; porownanie filtrowane vs niefiltrowane na tych samych rozliczonych okazjach',
          'progi_kandydackie_r':list(V162_PROGI),
          'brak_przewagi_oos':'Nawet dodatnia roznica wzgledem baseline nie potwierdza rentownosci',
          'ograniczenia':['Okno 7d i pojedynczy split: mala proba, bez wielokrotnych foldow',
            'Wybor na treningu optymalizuje sredni R, co moze przeuczyc filtr',
            'Koszt R to szacunek; brak pelnych historycznych arkuszy',
            'Rozne wielkosci populacji: porownuj zarowno sredni jak i sume R',
            'Brak automatycznej zmiany bramki, dziennika PROBA lub sygnalow']}
    return rows,meta



# V6.164 - wielokrotny prequential walk-forward; testy badawcze, bez zmian gate/PROBA.
V164_CUTS=(0.45,0.60,0.75)
V164_EXTRA_ROUNDTRIP_BPS=(0.0,5.0,10.0)
V164_PORTFOLIO_SLOTS=3

def abc_v164_metrics(triples):
    y=[float(t[2]) for t in triples]
    return {'n':len(y),'suma_R':sum(y),'sredni_R':sum(y)/len(y) if y else None,
            'zyskowne':sum(x>0 for x in y)}

def abc_v164_portfolio(triples, max_slots=V164_PORTFOLIO_SLOTS):
    # Slot rezerwowany od chwili sygnalu do czasu wyjscia; selekcja nie uzywa przyszlego R.
    # Wylacznie rozliczone sygnaly: to nie pelny portfel live z nierozliczonymi zleceniami.
    ordered=sorted(triples,key=lambda t:(abc_v162_czas_ms(t[0].get('snapshot_utc')) or 0,
                                        str(t[0].get('symbol'))))
    active=[]; accepted=[]; omitted=0; unknown=0
    for t in ordered:
        r=t[0]; start=abc_v162_czas_ms(r.get('snapshot_utc'))
        end=abc_v162_czas_ms(r.get('exit_utc'))
        if start is None or end is None or end<start:
            unknown+=1;continue
        active=[e for e in active if e>start]
        if len(active)>=max_slots: omitted+=1;continue
        active.append(end); accepted.append(t)
    return accepted,{'pominiete_brak_czasu':unknown,'pominiete_brak_slotu':omitted,'max_sloty':max_slots}

def abc_v164_stress_cost(r, result, extra_bps):
    try:
        entry=float(r.get('entry_snapshot'));sl=float(r.get('sl'))
        distance=abs(entry-sl)/entry
        if not np.isfinite(distance) or distance<=0: return None
        # Przyblizenie ex-post, nie ponowna symulacja wykonania.
        return float(result)-(extra_bps/10000)/distance
    except (ValueError,TypeError,ZeroDivisionError):return None

def abc_v164_wf(all_rows):
    unique=abc_v162_unikaty(all_rows)
    dated=sorted((r for r in unique if abc_v162_czas_ms(r.get('snapshot_utc')) is not None),
                 key=lambda r:abc_v162_czas_ms(r.get('snapshot_utc')))
    if len(dated)<3:
        return [],{'wersja':'V6.164','status':'BRAK_PROBY','unikalne':len(unique)}
    records=[]
    cuts=sorted(set(abc_v162_czas_ms(dated[min(len(dated)-1,int(len(dated)*frac))].get('snapshot_utc')) for frac in V164_CUTS))
    for fold,cut in enumerate(cuts,1):
        next_cut=cuts[fold] if fold<len(cuts) else None
        train=[];test=[];overlap=0
        for r in dated:
            signal=abc_v162_czas_ms(r.get('snapshot_utc'))
            if signal<cut:
                end=abc_v162_czas_ms(r.get('exit_utc'))
                if end is not None and end<cut:train.append(r)
                else:overlap+=1
            elif next_cut is None or signal<next_cut:test.append(r)
        for market in ('CRYPTO','TRADFI','RAZEM'):
            sel=lambda arr:[r for r in arr if market=='RAZEM' or r.get('rynek_v160')==market]
            a,b=sel(train),sel(test)
            candidates=[abc_v162_statystyki(a,t) for t in V162_PROGI]
            good=[v for v in candidates if v['liczba']>=8 and v['dni_sygnalow']>=2]
            best=max(good,key=lambda v:(v['sredni_R_netto'],-v['prog_r'])) if good else None
            base=abc_v162_rozliczone(b)
            if best is None:
                records.append(dict(fold=fold,rynek=market,cut_utc=snap_iso(cut),test_koniec_utc=snap_iso(next_cut) if next_cut else None,
                    status='BRAK_PROGU_Z_TRENINGU',train_n=len(abc_v162_rozliczone(a)),train_overlap_pominiete=overlap,
                    test_n=len(base),prog_r=None))
                continue
            filtered=[t for t in base if t[1]<=best['prog_r']]
            rejected=[t for t in base if t[1]>best['prog_r']]
            assert len(base)==len(filtered)+len(rejected)
            for name,tr in (('BEZ_FILTRA',base),('PROG_KOSZT',filtered)):
                portfolio,diag=abc_v164_portfolio(tr)
                for extra in V164_EXTRA_ROUNDTRIP_BPS:
                    stressed=[abc_v164_stress_cost(r,y,extra) for r,_,y in portfolio]
                    values=[v for v in stressed if v is not None and np.isfinite(v)]
                    records.append(dict(fold=fold,rynek=market,cut_utc=snap_iso(cut),
                      test_koniec_utc=snap_iso(next_cut) if next_cut else None,
                      status='BADAWCZY_BEZ_POTWIERDZENIA_PRZEWAGI',wariant=name,prog_r=best['prog_r'],
                      train_n=len(abc_v162_rozliczone(a)),train_overlap_pominiete=overlap,
                      test_n=len(base),odrzucone_n=len(rejected) if name=='PROG_KOSZT' else 0,
                      test_wybrane_n=len(tr),portfel_n=len(portfolio),
                      portfel_bez_slotu_n=diag['pominiete_brak_slotu'],
                      portfel_brak_czasu_n=diag['pominiete_brak_czasu'],
                      dodatkowy_roundtrip_bps=extra,
                      wynik_n=len(values),suma_R=sum(values),sredni_R=sum(values)/len(values) if values else None))
    meta={'wersja':'V6.164','status':'BADAWCZY_NIE_POTWIERDZA_PRZEWAGI',
          'unikalne_okazje':len(unique),'foldy':len(cuts),'proporcje_ciecia':list(V164_CUTS),
          'okna_testu':'rozlaczne od daty cut do kolejnej daty cut; ostatni fold do konca danych',
          'train':'rosnace okno; tylko sygnaly z wyjsciem znanym przed cut',
          'progi':list(V162_PROGI),'stress_bps':list(V164_EXTRA_ROUNDTRIP_BPS),
          'portfolio_slots':V164_PORTFOLIO_SLOTS,
          'ograniczenia':['Tylko historycznie rozliczone okazje; pomija rezerwacje zlecen jeszcze otwartych i czekajacych, wynik portfela optymistyczny',
            'Stres kosztu to zmiana wyniku R bez ponownego wykonania transakcji; nie jest pelnym backtestem egzekucji',
            'Foldy z jednego 7-dniowego okresu nie sa niezaleznym OOS',
            'Nie zmienia sygnalow, gate, do_handlu, PROBA ani ryzyka']}
    return records,meta


# V6.165: event-driven ABC research portfolio. No gate, risk, or live trade changes.
def abc_v165_event_portfolio(rows, cut, end=None, threshold=None, max_slots=3):
    """Replay reservations from signals, not just settled results.

    Pre-cut positions occupy slots across cut; only post-cut signals are scored.
    Unknown exits retain their slots. MARKET research signal reserves immediately.
    """
    ordered=sorted(rows,key=lambda r:(abc_v162_czas_ms(r.get('snapshot_utc')) or -1,
                                  str(r.get('symbol') or ''),str(r.get('side') or '')))
    active=[]; events=[]; selected=[]; max_seen=0
    for r in ordered:
        signal=abc_v162_czas_ms(r.get('snapshot_utc'))
        if signal is None or (end is not None and signal>=end): continue
        # Known exits may release capacity only after their exit time, NEVER using outcome for selection.
        active=[a for a in active if a['end'] is None or a['end']>signal]
        in_test=signal>=cut
        key=r.get('unikalny_abc_id_v162') or '|'.join(map(str,abc_v162_klucz(r) or ('UNKNOWN',signal)))
        try: cost=float(r.get('koszt_r_est_v160'))
        except (TypeError,ValueError): cost=float('nan')
        if in_test and threshold is not None and (not np.isfinite(cost) or cost>threshold):
            events.append(dict(abc_id=key,snapshot_utc=r.get('snapshot_utc'),rynek=r.get('rynek_v160'),
                               decyzja='ODRZUCONY_KOSZT',koszt_r=cost if np.isfinite(cost) else None,
                               status=r.get('status'),exit_utc=r.get('exit_utc'),r_netto=None,
                               aktywne_przed=len(active)))
            continue
        if len(active)>=max_slots:
            events.append(dict(abc_id=key,snapshot_utc=r.get('snapshot_utc'),rynek=r.get('rynek_v160'),
                               decyzja='ODRZUCONY_BRAK_SLOTU' if in_test else 'PRZED_CUT_BRAK_SLOTU',
                               koszt_r=cost if np.isfinite(cost) else None,status=r.get('status'),
                               exit_utc=r.get('exit_utc'),r_netto=None,aktywne_przed=len(active)))
            continue
        exit_ms=abc_v162_czas_ms(r.get('exit_utc'))
        if exit_ms is not None and exit_ms<signal: exit_ms=None
        # An open/unsettled signal is never assumed to close because its hypothetical TTL elapsed.
        if r.get('status') not in FINAL or r.get('luka_danych'):
            exit_ms=None
        active.append({'end':exit_ms,'start':signal,'pre_cut':not in_test})
        max_seen=max(max_seen,len(active))
        settled=bool(r.get('status') in FINAL and not r.get('luka_danych') and exit_ms is not None)
        try: pnl=float(r.get('r_netto')) if settled else None
        except (ValueError,TypeError): pnl=None
        if pnl is not None and not np.isfinite(pnl): pnl=None
        decision='PRZED_CUT_REZERWACJA' if not in_test else 'WYBRANY_ROZLICZONY' if pnl is not None else 'WYBRANY_NIEROZLICZONY'
        events.append(dict(abc_id=key,snapshot_utc=r.get('snapshot_utc'),rynek=r.get('rynek_v160'),
                           decyzja=decision,koszt_r=cost if np.isfinite(cost) else None,
                           status=r.get('status'),exit_utc=r.get('exit_utc'),r_netto=pnl if in_test else None,
                           aktywne_przed=len(active)-1))
        if in_test: selected.append(r)
    test_events=[e for e in events if e['decyzja'] in ('ODRZUCONY_KOSZT','ODRZUCONY_BRAK_SLOTU','WYBRANY_ROZLICZONY','WYBRANY_NIEROZLICZONY')]
    n_cost=sum(e['decyzja']=='ODRZUCONY_KOSZT' for e in test_events)
    n_slots=sum(e['decyzja']=='ODRZUCONY_BRAK_SLOTU' for e in test_events)
    n_settled=sum(e['decyzja']=='WYBRANY_ROZLICZONY' for e in test_events)
    n_pending=sum(e['decyzja']=='WYBRANY_NIEROZLICZONY' for e in test_events)
    n_signals=len(test_events)
    assert n_signals==n_cost+n_slots+n_settled+n_pending
    values=[e['r_netto'] for e in test_events if e['decyzja']=='WYBRANY_ROZLICZONY' and e['r_netto'] is not None]
    assert len(values)==n_settled and max_seen<=max_slots
    diag=dict(sygnaly=n_signals,odrzucone_koszt=n_cost,odrzucone_sloty=n_slots,
              wybrane_rozliczone=n_settled,wybrane_nierozliczone=n_pending,
              suma_R=sum(values),sredni_R=sum(values)/len(values) if values else None,
              maksimum_slotow=max_seen,pozycje_przed_cut=sum(e['decyzja']=='PRZED_CUT_REZERWACJA' for e in events),
              status='BADAWCZY_NIE_REKONSTRUKCJA_ZLECEN')
    return events,diag


def abc_v165_wf(all_rows):
    unique=abc_v162_unikaty(all_rows)
    dated=sorted((r for r in unique if abc_v162_czas_ms(r.get('snapshot_utc')) is not None),
                 key=lambda r:abc_v162_czas_ms(r.get('snapshot_utc')))
    if len(dated)<3:
        return [],[],{'wersja':'V6.165','status':'BRAK_PROBY'}
    cuts=sorted(set(abc_v162_czas_ms(dated[min(len(dated)-1,int(len(dated)*f))].get('snapshot_utc')) for f in V164_CUTS))
    reports=[];details=[]
    for fold,cut in enumerate(cuts,1):
        end=cuts[fold] if fold<len(cuts) else None
        for market in ('CRYPTO','TRADFI','RAZEM'):
            market_rows=[r for r in dated if market=='RAZEM' or r.get('rynek_v160')==market]
            train=[]
            for r in market_rows:
                sig=abc_v162_czas_ms(r.get('snapshot_utc'))
                ex=abc_v162_czas_ms(r.get('exit_utc'))
                if sig<cut and ex is not None and ex<cut:train.append(r)
            candidates=[abc_v162_statystyki(train,t) for t in V162_PROGI]
            eligible=[c for c in candidates if c['liczba']>=8 and c['dni_sygnalow']>=2]
            best=max(eligible,key=lambda c:(c['sredni_R_netto'],-c['prog_r'])) if eligible else None
            for variant, threshold in (('BEZ_FILTRA',None),('Z_FILTREM',best['prog_r'] if best else None)):
                if variant=='Z_FILTREM' and best is None:
                    reports.append(dict(fold=fold,rynek=market,wariant=variant,prog_r=None,
                                        status='BRAK_PROGU_Z_TRAIN',train_n=len(abc_v162_rozliczone(train))))
                    continue
                events,d=abc_v165_event_portfolio(market_rows,cut,end,threshold,V164_PORTFOLIO_SLOTS)
                reports.append(dict(fold=fold,rynek=market,wariant=variant,
                    cut_utc=snap_iso(cut),test_koniec_utc=snap_iso(end) if end else None,
                    prog_r=threshold,train_n=len(abc_v162_rozliczone(train)),**d))
                for e in events:
                    if e['decyzja']!='PRZED_CUT_REZERWACJA' and e['decyzja']!='PRZED_CUT_BRAK_SLOTU':
                        details.append(dict(fold=fold,wariant=variant,rynek_analizy=market,prog_r=threshold,**e))
    return reports,details,{'wersja':'V6.165','status':'TYLKO_BADAWCZY',
        'unikalne_okazje':len(unique),'foldy':len(cuts),
        'sloty':V164_PORTFOLIO_SLOTS,
        'metoda':'Rezerwacja od snapshotu sygnalu do znanego wyjscia; nierozliczone trzymaja slot. Pozycje sprzed cut pozostaja aktywne w foldzie; tylko pozniejsze sygnaly punktowane.',
        'ograniczenia':['Brak rzeczywistego dziennika fill: hipotetyczny MARKET rezerwuje slot od sygnalu',
          'Brak dokladnych historycznych zlecen LIMIT dla nowych samodzielnych ABC',
          'Foldy z 7 dni to nie niezalezny OOS',
          'Odtwarzanie z kanonicznych 3/6/12h snapshotow pomija sygnaly pomiedzy nimi',
          'Status nierozliczony bez exit_utc trzyma slot do konca, konserwatywnie',
          'Nie modyfikuje gate, PROBA, do_handlu ani realizacji zlecen']}



# V6.166 — One continuous OOS event stream per market and strategy, with time-causal fold boundaries.
def abc_v166_portfolio(dated, cuts, chosen, variant='BEZ_FILTRA', max_slots=3):
    """Reserve at signal (MARKET research); release only on known exit, never on future PnL.

    For a selected signal, score PnL in the exit fold, not the entry fold.
    All timestamps are UTC milliseconds. Closed positions across cut are not counted active.
    """
    cuts=sorted(set(int(x) for x in cuts))
    assert cuts and max_slots>=1
    rows=sorted((r for r in dated if abc_v162_czas_ms(r.get('snapshot_utc')) is not None),
                key=lambda r:(abc_v162_czas_ms(r['snapshot_utc']),str(r.get('symbol'))))
    # Active entries include only reservations from selected signals.
    active=[]; chosen_entries=[]; events=[]; maximum=0
    for r in rows:
        signal=abc_v162_czas_ms(r['snapshot_utc'])
        # Any known exit at this instant releases its slot first.
        active=[a for a in active if a['exit_ms'] is None or a['exit_ms']>signal]
        fold=sum(signal>=x for x in cuts)-1
        is_test=fold>=0
        threshold=chosen.get(fold) if is_test else None
        key=r.get('unikalny_abc_id_v162') or '|'.join(map(str,abc_v162_klucz(r) or ('UNKNOWN',signal)))
        raw_cost=r.get('koszt_r_est_v160')
        try: cost=float(raw_cost)
        except (TypeError,ValueError): cost=float('nan')
        before=len(active)
        if is_test and variant=='Z_FILTREM' and threshold is None:
            decision='BRAK_PROGU_Z_TRAIN'
        elif is_test and variant=='Z_FILTREM' and (not np.isfinite(cost) or cost>threshold):
            decision='ODRZUCONY_KOSZT'
        elif before>=max_slots:
            decision='ODRZUCONY_BRAK_SLOTU' if is_test else 'ROZGRZEWKA_BRAK_SLOTU'
        else:
            ex=abc_v162_czas_ms(r.get('exit_utc'))
            settled=bool(r.get('status') in FINAL and not r.get('luka_danych') and ex is not None and ex>=signal)
            if not settled: ex=None
            try: pnl=float(r.get('r_netto')) if settled else None
            except (ValueError,TypeError): pnl=None
            if pnl is not None and not np.isfinite(pnl): pnl=None
            if pnl is None: settled=False
            active.append({'exit_ms':ex,'start_ms':signal,'key':key})
            chosen_entries.append({'key':key,'start_ms':signal,'exit_ms':ex,'r_netto':pnl,'entry_fold':fold})
            maximum=max(maximum,len(active))
            decision=('WYBRANY_ROZLICZONY' if settled else 'WYBRANY_NIEROZLICZONY') if is_test else 'ROZGRZEWKA_REZERWACJA'
        selected=decision in ('WYBRANY_ROZLICZONY','WYBRANY_NIEROZLICZONY','ROZGRZEWKA_REZERWACJA')
        selected_entry=chosen_entries[-1] if selected else None
        events.append(dict(abc_id=key,snapshot_utc=r['snapshot_utc'],fold=fold,variant=variant,
            decyzja=decision,aktywne_przed=before,aktywne_po=len(active),
            koszt_r=cost if np.isfinite(cost) else None,
            exit_utc=snap_iso(selected_entry['exit_ms']) if selected_entry and selected_entry['exit_ms'] is not None else None,
            r_netto=selected_entry['r_netto'] if selected_entry else None,
            pochodzenie='ROZGRZEWKA' if fold<0 else 'OOS',
            wybrany=int(selected),status_zrodla=r.get('status')))
    # Exact occupancy at cut includes signals before cut whose exits are unknown or strictly later.
    occupancy=[]
    for i,cut in enumerate(cuts):
        occupied=[a for a in chosen_entries if a['start_ms']<cut and (a['exit_ms'] is None or a['exit_ms']>cut)]
        occupancy.append(dict(fold=i+1,cut_utc=snap_iso(cut),aktywnych_na_granicy=len(occupied),
                              abc_id_aktywnych=';'.join(a['key'] for a in occupied)))
        assert len(occupied)<=max_slots
    reports=[]
    for i,cut in enumerate(cuts):
        nxt=cuts[i+1] if i+1<len(cuts) else None
        chosen_events=[e for e in events if e['fold']==i]
        completed=[a for a in chosen_entries if a['exit_ms'] is not None and a['exit_ms']>=cut and (nxt is None or a['exit_ms']<nxt) and a['r_netto'] is not None]
        # Exit-fold attribution includes selections from earlier folds, including warmup: mark explicitly.
        from_before=[a for a in completed if a['start_ms']<cut]
        # For clean OOS PnL exclude warmup-origin trades; do keep pre-cut selected *test* trades.
        oos=[a for a in completed if a['entry_fold']>=0]
        n_selected=sum(e['decyzja'] in ('WYBRANY_ROZLICZONY','WYBRANY_NIEROZLICZONY') for e in chosen_events)
        rejected_cost=sum(e['decyzja']=='ODRZUCONY_KOSZT' for e in chosen_events)
        rejected_slot=sum(e['decyzja']=='ODRZUCONY_BRAK_SLOTU' for e in chosen_events)
        no_threshold=sum(e['decyzja']=='BRAK_PROGU_Z_TRAIN' for e in chosen_events)
        assert len(chosen_events)==n_selected+rejected_cost+rejected_slot+no_threshold
        vals=[a['r_netto'] for a in oos]
        reports.append(dict(fold=i+1,cut_utc=snap_iso(cut),test_koniec_utc=snap_iso(nxt) if nxt else None,
            wariant=variant,prog_r=chosen.get(i) if variant=='Z_FILTREM' else None,
            sygnaly=len(chosen_events),wybrane=n_selected,odrzucone_koszt=rejected_cost,
            odrzucone_sloty=rejected_slot,bez_progu=no_threshold,
            aktywnych_na_poczatku=occupancy[i]['aktywnych_na_granicy'],
            zamkniecia_w_oknie=len(completed),zamkniecia_z_poprzednich_okien=len(from_before),
            rozliczone_oos=len(oos),suma_R=sum(vals),sredni_R=sum(vals)/len(vals) if vals else None,
            maksimum_slotow=maximum,status='BADAWCZY'))
    assert maximum<=max_slots
    return reports,events,occupancy


def abc_v166_wf(all_rows):
    unique=abc_v162_unikaty(all_rows)
    dated=sorted((r for r in unique if abc_v162_czas_ms(r.get('snapshot_utc')) is not None),
                 key=lambda r:abc_v162_czas_ms(r['snapshot_utc']))
    if len(dated)<3: return [],[],[],{'wersja':'V6.166','status':'BRAK_PROBY'}
    cuts=sorted(set(abc_v162_czas_ms(dated[min(len(dated)-1,int(len(dated)*f))]['snapshot_utc']) for f in V164_CUTS))
    reports=[]; events=[]; occupancy=[]; events_all=[]
    for market in ('CRYPTO','TRADFI','RAZEM'):
        rows=[r for r in dated if market=='RAZEM' or r.get('rynek_v160')==market]
        chosen={}
        for i,cut in enumerate(cuts):
            train=[r for r in rows if abc_v162_czas_ms(r['snapshot_utc'])<cut
                   and (ex:=abc_v162_czas_ms(r.get('exit_utc'))) is not None and ex<cut]
            eligible=[abc_v162_statystyki(train,p) for p in V162_PROGI]
            good=[v for v in eligible if v['liczba']>=8 and v['dni_sygnalow']>=2]
            best=max(good,key=lambda v:(v['sredni_R_netto'],-v['prog_r'])) if good else None
            chosen[i]=best['prog_r'] if best else None
        for variant in ('BEZ_FILTRA','Z_FILTREM'):
            rs,es,os=abc_v166_portfolio(rows,cuts,chosen,variant,V164_PORTFOLIO_SLOTS)
            reports += [dict(rynek=market,**r) for r in rs]
            events += [dict(rynek_analizy=market,**e) for e in es if e['fold']>=0]
            events_all += [dict(rynek_analizy=market,**e) for e in es]
            occupancy += [dict(rynek=market,wariant=variant,**o) for o in os]
    return reports,events,occupancy,{
        'wersja':'V6.169','status':'BADAWCZY_BEZ_POTWIERDZONEJ_PRZEWAGI',
        'unikalne_okazje':len(unique),'podzialy_utc':[snap_iso(x) for x in cuts],
        'metoda':'Jeden ciagly strumien selekcji na wariant i rynek; slot od sygnalu do potwierdzonego exit, bez resetowania miedzy foldami; R przypisywany do okna czasu wyjscia.',
        'ograniczenia':['Brak rzeczywistych fill i historycznego arkusza; symulacja sygnalow MARKET',
            'Rozgrzewka sprzed pierwszego cut rezerwuje miejsca, lecz jej PnL jest wylaczony z OOS',
            'Zmiana progu w foldzie dotyczy tylko nowych wejsc; pozycje w toku nie sa zamykane',
            'Okna z tego samego krotkiego zakresu nie sa niezaleznym dowodem przewagi',
            'Nie modyfikuje live gate, PROBA, rankingow ani do_handlu']},events_all



# V6.167 — audyt wybranych pozycji po osi czasu, w tym rozgrzewki.
def abc_v167_audyt(events_all, occupancy, reports, max_slots=3):
    """Niezaleznie od licznika selection sprawdza nakladanie przedzialow [entry, exit).

    Brak exit oznacza pozycje otwarta do konca obserwacji. Zdarzenia o tym samym
    czasie: zwolnienie miejsca (exit) przed rezerwacja (entry).
    """
    positions=[]; boundaries=[]; controls=[]
    groups=sorted(set((e['rynek_analizy'],e['variant']) for e in events_all))
    for market,variant in groups:
        es=[e for e in events_all if (e['rynek_analizy'],e['variant'])==(market,variant)]
        occ=[o for o in occupancy if (o['rynek'],o['wariant'])==(market,variant)]
        rep=[r for r in reports if (r['rynek'],r['wariant'])==(market,variant)]
        intervals=[]
        for e in es:
            if not e.get('wybrany'): continue
            start=abc_v162_czas_ms(e['snapshot_utc'])
            end=abc_v162_czas_ms(e.get('exit_utc'))
            if start is None or (end is not None and end<start):
                raise AssertionError('V167: niepoprawne daty pozycji '+str(e['abc_id']))
            intervals.append((start,end,e))
            exit_fold=next((r['fold'] for r in rep if end is not None and
                (abc_v162_czas_ms(r['cut_utc'])<=end) and
                (r.get('test_koniec_utc') is None or end<abc_v162_czas_ms(r['test_koniec_utc']))),None)
            positions.append(dict(rynek=market,wariant=variant,abc_id=e['abc_id'],
                wejscie_utc=e['snapshot_utc'],wyjscie_utc=e.get('exit_utc'),
                fold_wejscia=e['fold'],fold_wyjscia=exit_fold,
                pochodzenie=e['pochodzenie'],status_zrodla=e['status_zrodla'],
                rozliczona=int(end is not None and e.get('r_netto') is not None),
                r_netto=e.get('r_netto'),
                pnl_do_oos=int(e['fold']>=0 and exit_fold is not None and e.get('r_netto') is not None),
                przeniesiona_miedzy_oknami=int(exit_fold is not None and exit_fold!=e['fold']),
                pozostaje_otwarta=int(end is None)))
        # Independent capacity audit at EACH selected entry, in the exact signal
        # order recorded by the portfolio.  A zero-duration [t,t) interval
        # occupies a slot while processed but is not active at any later event,
        # including another signal sharing timestamp t. A naive time-only sweep
        # (+1 before -1) falsely counts it alongside other same-time entries.
        # Conversely a naive (-1 before +1) produces negative occupancy.
        peak=0
        prior=[]
        for start,end,e in intervals:
            existing=sum(1 for st,ex,_ in prior if (ex is None or ex>start))
            at_entry=existing+1
            peak=max(peak,at_entry)
            if at_entry>max_slots:
                raise AssertionError(f'V169: sloty {market}/{variant}: {at_entry} na {snap_iso(start)} ({e["abc_id"]})')
            prior.append((start,end,e))
        # Check intervals independently of portfolio counters at all distinct
        # timestamp boundaries, under [start,end) semantics. A zero-duration
        # position contributes only to its own instantaneous entry check above.
        timestamps=sorted({t for start,end,_ in intervals for t in (start,end) if t is not None})
        for t in timestamps:
            occupied=sum(1 for st,ex,_ in intervals if st<=t and (ex is None or ex>t))
            if occupied>max_slots:
                raise AssertionError(f'V169: przedzialy {market}/{variant}: {occupied} na {snap_iso(t)}')
            peak=max(peak,occupied)
        for o in occ:
            cut=abc_v162_czas_ms(o['cut_utc'])
            actual=[e['abc_id'] for start,end,e in intervals if start<cut and (end is None or end>cut)]
            reported=int(o['aktywnych_na_granicy'])
            ok=reported==len(actual) and sorted(actual)==sorted(filter(None,str(o.get('abc_id_aktywnych','')).split(';')))
            boundaries.append(dict(rynek=market,wariant=variant,fold=o['fold'],cut_utc=o['cut_utc'],
                aktywnych_niezaleznie=len(actual),aktywnych_raport=reported,
                abc_id_aktywnych=';'.join(actual),kontrola='ZGODNE' if ok else 'ROZBIEZNOSC'))
            if not ok: raise AssertionError('V167: granica '+str(o))
        for r in rep:
            fold=r['fold']
            pos=[p for p in positions if p['rynek']==market and p['wariant']==variant and p['fold_wyjscia']==fold]
            counted=[p for p in pos if p['pnl_do_oos']]
            warmup=[p for p in pos if p['pochodzenie']=='ROZGRZEWKA']
            actual=sum(float(p['r_netto']) for p in counted)
            ok=(len(counted)==int(r['rozliczone_oos']) and len(pos)==int(r['zamkniecia_w_oknie'])
                and abs(actual-float(r['suma_R']))<1e-8 and peak==int(r['maksimum_slotow']))
            controls.append(dict(rynek=market,wariant=variant,fold=fold,
                rozliczone_oos=len(counted),rozliczone_oos_raport=r['rozliczone_oos'],
                rozgrzewka_zamknieta=len(warmup),wszystkie_zamkniecia=len(pos),
                suma_R_oos=actual,suma_R_raport=r['suma_R'],maksimum_slotow_niezaleznie=peak,
                maksimum_slotow_raport=r['maksimum_slotow'],kontrola='ZGODNE' if ok else 'ROZBIEZNOSC'))
            if not ok: raise AssertionError('V167: raport '+str(controls[-1]))
    return positions,boundaries,controls


def test_v167():
    base=pd.Timestamp('2026-10-20T00:00:00Z')
    def rec(i,a,b=None):
        return dict(symbol='S'+str(i),side='LONG',elliott_abc_c_utc_ms=100+i,
            snapshot_utc=(base+pd.Timedelta(hours=a)).isoformat(),
            exit_utc=(base+pd.Timedelta(hours=b)).isoformat() if b is not None else None,
            status='TP1' if b is not None else 'otwarty',r_netto=.5 if b is not None else None,
            koszt_r_est_v160=.2)
    cuts=[abc_v162_czas_ms(base+pd.Timedelta(hours=h)) for h in (10,20)]
    rows=[rec(1,1,12),rec(2,3,25),rec(3,13,25),rec(4,21,22),rec(5,26,None)]
    rs,es,oc=abc_v166_portfolio(rows,cuts,{0:.5,1:.5},max_slots=2)
    es=[dict(rynek_analizy='CRYPTO',**e) for e in es]
    oc=[dict(rynek='CRYPTO',wariant='BEZ_FILTRA',**o) for o in oc]
    rs=[dict(rynek='CRYPTO',**r) for r in rs]
    ps,bs,cs=abc_v167_audyt(es,oc,rs,max_slots=2)
    assert all(x['kontrola']=='ZGODNE' for x in bs+cs)
    assert any(x['pochodzenie']=='ROZGRZEWKA' and x['fold_wyjscia']==1 and not x['pnl_do_oos'] for x in ps)
    assert any(x['fold_wejscia']==0 and x['fold_wyjscia']==2 and x['pnl_do_oos'] for x in ps)
    # Regression: the real scan can close a MARKET position at the exact
    # signal timestamp.  The audit must not release that slot before opening.
    same=[rec(90,11,11),rec(91,12,14)]
    rr,ee,oo=abc_v166_portfolio(same,cuts,{0:.5,1:.5},max_slots=2)
    rr=[dict(rynek='CRYPTO',**r) for r in rr]
    ee=[dict(rynek_analizy='CRYPTO',**e) for e in ee]
    oo=[dict(rynek='CRYPTO',wariant='BEZ_FILTRA',**o) for o in oo]
    pp,bb,cc=abc_v167_audyt(ee,oo,rr,max_slots=2)
    assert len(pp)==2 and all(x['kontrola']=='ZGODNE' for x in bb+cc)
    # Regression on real failure shape: instant exit followed by same-time
    # entries must not create a fictitious fourth active slot.
    instant=[rec(92,11,11),rec(93,11,16),rec(94,11,17),rec(95,11,18)]
    ir,ie,io=abc_v166_portfolio(instant,cuts,{0:.5,1:.5},max_slots=3)
    assert sum(e['wybrany'] for e in ie)==4,ie
    ir=[dict(rynek='CRYPTO',**r) for r in ir]
    ie=[dict(rynek_analizy='CRYPTO',**e) for e in ie]
    io=[dict(rynek='CRYPTO',wariant='BEZ_FILTRA',**o) for o in io]
    _,ib,ic=abc_v167_audyt(ie,io,ir,max_slots=3)
    assert all(x['kontrola']=='ZGODNE' for x in ib+ic)
    # Independent test: fabricated overlapping reservations must be rejected.
    forged=[dict(x) for x in es]
    forged.append(dict(forged[-1],abc_id='FAKE',snapshot_utc=rows[2]['snapshot_utc'],wybrany=1))
    try: abc_v167_audyt(forged,oc,rs,max_slots=2)
    except AssertionError: pass
    else: raise AssertionError('V167: niewykryte przekroczenie limitu')
    return 'PASS (warmup != OOS, carry, wyjscia, kontrola przedzialow, wykrywanie naruszenia limitu)'

def test_v166():
    origin=pd.Timestamp('2026-10-20T00:00:00Z')
    def rec(i,start,exit_at=None,status='TP1',cost=.2):
        return dict(symbol='TEST'+str(i),side='LONG',elliott_abc_c_utc_ms=1000+i,
            snapshot_utc=(origin+pd.Timedelta(hours=start)).isoformat(),
            exit_utc=(origin+pd.Timedelta(hours=exit_at)).isoformat() if exit_at is not None else None,
            status=status,r_netto=.5 if status=='TP1' else None,koszt_r_est_v160=cost,rynek_v160='CRYPTO')
    cut1=abc_v162_czas_ms(origin+pd.Timedelta(hours=10));cut2=abc_v162_czas_ms(origin+pd.Timedelta(hours=20))
    rows=[rec(1,1,25),rec(2,2,status='otwarty'),rec(3,11,12),rec(4,22,23),rec(5,26,27)]
    reports,events,occ=abc_v166_portfolio(rows,[cut1,cut2],{0:0.5,1:0.5},max_slots=2)
    assert occ[0]['aktywnych_na_granicy']==2 and occ[1]['aktywnych_na_granicy']==2,occ
    assert reports[0]['odrzucone_sloty']==1 and reports[1]['odrzucone_sloty']==1,reports
    assert reports[1]['wybrane']==1 and reports[1]['rozliczone_oos']==1,reports
    # No pre-cut profit is attributed to OOS even when a warmup trade closes after cut.
    assert sum(r['rozliczone_oos'] for r in reports)==1,reports
    # A test-origin exit is assigned to the exit fold, not signal fold.
    alt=[rec(10,11,21),rec(11,15,status='otwarty')]
    rr,_,_=abc_v166_portfolio(alt,[cut1,cut2],{0:.5,1:.5},max_slots=2)
    assert rr[0]['rozliczone_oos']==0 and rr[1]['rozliczone_oos']==1,rr
    return 'PASS (carry-over, cut occupancy, closed PnL by exit, no warmup PnL, max slots)'


def test_v165():
    def rec(i,minute,status='TP1',end_min=None,cost=.2):
        start=pd.Timestamp('2026-10-25T00:00:00Z')+pd.Timedelta(minutes=minute)
        end=start+pd.Timedelta(minutes=end_min) if end_min is not None else None
        return dict(symbol=f'C{i}',side='LONG',elliott_abc_c_utc_ms=1000+i,
            snapshot_utc=start.isoformat(),exit_utc=end.isoformat() if end is not None else None,
            status=status,r_netto=.5 if status=='TP1' else None,
            koszt_r_est_v160=cost,rynek_v160='CRYPTO')
    cut=abc_v162_czas_ms(pd.Timestamp('2026-10-25T00:10:00Z'))
    rows=[rec(1,0,end_min=30),rec(2,5,status='otwarty'),rec(3,12,end_min=10),
          rec(4,14,end_min=2),rec(5,31,end_min=2),rec(6,33,end_min=2,cost=.9)]
    events,d=abc_v165_event_portfolio(rows,cut,max_slots=2,threshold=.5)
    assert d['odrzucone_sloty']==2 and d['odrzucone_koszt']==1 and d['wybrane_rozliczone']==1,d
    assert d['maksimum_slotow']==2 and d['pozycje_przed_cut']==2
    assert d['sygnaly']==4
    no,diag=abc_v165_event_portfolio(rows,cut,max_slots=2)
    assert diag['odrzucone_sloty']==2 and diag['wybrane_rozliczone']==2,diag
    assert all(x['decyzja']!='WYBRANY_ROZLICZONY' for x in events if x['status']=='otwarty')
    return 'PASS (carry-over, unresolved reservations, cost-first, slot cap, reconciliation)'

def test_v164():
    def rec(i, day, cost=.2, r=.4, exit_day=None):
        return dict(symbol=f'COIN{i}',side='LONG',elliott_abc_c_utc_ms=1000+i,krok_h=3,
          snapshot_utc=f'2026-10-{day:02d}T00:00',exit_utc=f'2026-10-{exit_day or day:02d}T03:00',
          status='TP1',r_netto=r,koszt_r_est_v160=cost,rynek_v160='CRYPTO',entry_snapshot=100.,sl=99.)
    data=[rec(i,20+i//3,cost=.2 if i%2 else .6) for i in range(21)]
    out,meta=abc_v164_wf(data)
    assert meta['wersja']=='V6.164' and len(out)>0
    for x in out:
        if x.get('wariant'):
            assert x['portfel_n']<=x['test_wybrane_n'] and x['wynik_n']<=x['portfel_n']
    val=abc_v164_stress_cost(data[0],1.,10.)
    assert abs(val-.9)<1e-8
    selected,diag=abc_v164_portfolio([(rec(i,20),.2,.4) for i in range(5)],max_slots=2)
    assert len(selected)==2 and diag['pominiete_brak_slotu']==3
    return 'PASS (wiele foldow, koszt bps/R, limity portfela, brak przecieku wynikow)'

def test_v163():
    def rec(i,day,ret,cost,market='CRYPTO',exit_day=None):
        return dict(symbol=f'S{i}',side='LONG',elliott_abc_c_utc_ms=100000+i,
          krok_h=3,snapshot_utc=f'2026-10-{day:02d}T00:00:00Z',
          exit_utc=f'2026-10-{exit_day or day:02d}T01:00:00Z',
          rynek_v160=market,status='SL' if ret<0 else 'TP1',r_netto=ret,koszt_r_est_v160=cost)
    data=[rec(i,20+i//3, 0.2 if i%2 else -0.1, 0.2 if i%3 else 0.6) for i in range(15)]
    # Ensure a training portion with 2 days, and no contamination after split.
    result,meta=abc_v163_porownanie(data)
    assert len(result)==6 and meta['wersja']=='V6.163'
    for row in result:
        if row['prog_wybrany_train_r'] is not None:
            assert row['bez_filtra_n']==row['z_filtrem_n']+row['odrzucone_przez_filtr_n']
            assert abs(row['bez_filtra_suma_R']-row['z_filtrem_suma_R']-row['odrzucone_suma_R'])<1e-8
    no, _=abc_v163_porownanie([])
    assert no==[]
    return 'PASS (wspolna populacja, sumy, brak progu, pusta proba)'

def test_v162():
    def rec(symbol,day,c,ret,cost,step=3,exit_day=None):
        return dict(symbol=symbol,side='LONG',elliott_abc_c_utc_ms=c,krok_h=step,
                    snapshot_utc=f'2026-10-{day:02d}T00:00:00Z',
                    exit_utc=f'2026-10-{(exit_day or day):02d}T02:00:00Z',
                    rynek_v160='CRYPTO',status='SL' if ret<0 else 'TP1',
                    r_netto=ret,koszt_r_est_v160=cost)
    records=[rec('A',21,123,-1,.2,12),rec('A',22,123,1,.2,3),
             rec('B',23,456,1,.6),rec('C',24,789,-1,.4)]
    u,g,w,m=abc_v162_analiza(records)
    assert len(u)==3 and len(g)==12 and len(w)>=3
    assert next(x for x in g if x['rynek']=='RAZEM' and x['prog_r']==.25)['liczba']==1
    assert m['liczba_obserwacji_w_siatkach']==4
    assert all(x['status']=='NIE_OCENIANO_BRAK_PROGU_W_TRAIN' for x in w if x['zbior']=='TEST_OOS_WEWNETRZNY')
    return 'PASS (unikaty, progi, chronologia, brak arbitralnego wyboru przy malej probie)'

def raport_qc_przyczyn_v176(kontrola, powtarzalne, przyczyny):
    """Tylko odczyt istniejacych raportow QC; zero zmian w decyzjach handlowych."""
    index = {(str(r.get('symbol')), str(r.get('interwal'))): r
             for r in (kontrola or []) if isinstance(r, dict)}
    repeats = {(str(r.get('symbol')), str(r.get('interwal'))): r
               for r in (powtarzalne or []) if isinstance(r, dict)}
    reasons = {}
    for r in (przyczyny or []):
        if not isinstance(r, dict):
            continue
        key = (str(r.get('symbol')), str(r.get('interwal')))
        reasons.setdefault(key, []).append(r)
    out=[]
    for key in sorted(set(repeats) | set(reasons)):
        qc=index.get(key,{})
        repeat=repeats.get(key,{})
        entries=reasons.get(key,[]) or [{'powod':'NIEUSTALONY','liczba':0}]
        blocked=bool(qc.get('blokuje_analize',repeat.get('blokuje_analize',False)))
        missing=int(qc.get('brakuje',0) or 0)
        for reason in sorted(entries,key=lambda a:str(a.get('powod',''))):
            label=('BLOKADA_WG_QC' if blocked else
                   'ODRZUCENIE_BEZ_BLOKADY')
            out.append({'symbol':key[0], 'interwal':key[1],
                        'status_historii':str(qc.get('status','')),
                        'powod_odrzucenia':str(reason.get('powod','NIEUSTALONY')),
                        'liczba_odrzuconych':int(reason.get('liczba',0) or 0),
                        'kolejne_uruchomienia':int(repeat.get('kolejne_uruchomienia_z_odrzuceniami',0) or 0),
                        'blokuje_analize':blocked,
                        'powod_blokady':str(qc.get('blokada_powod') or repeat.get('powod_blokady') or ''),
                        'brakuje_swiec':missing,'ocena_diagnostyczna':label})
    return out


def test_v176():
    qc=[{'symbol':'ABCUSDT','interwal':'1h','status':'NATURALNIE_KROTKA_POTWIERDZONA',
         'blokuje_analize':True,'blokada_powod':'ZA_MALO_SWIEC','brakuje':23},
        {'symbol':'ABCUSDT','interwal':'15m','status':'ODRZUCONE_SWIECE',
         'blokuje_analize':False,'brakuje':0}]
    repeats=[{'symbol':'ABCUSDT','interwal':'1h','kolejne_uruchomienia_z_odrzuceniami':89},
             {'symbol':'ABCUSDT','interwal':'15m','kolejne_uruchomienia_z_odrzuceniami':89}]
    causes=[{'symbol':'ABCUSDT','interwal':'1h','powod':'LOW_NIEDODATNIE','liczba':16},
            {'symbol':'ABCUSDT','interwal':'15m','powod':'LOW_NIEDODATNIE','liczba':67}]
    before=[dict(x) for x in qc]
    rows=raport_qc_przyczyn_v176(qc,repeats,causes)
    assert len(rows)==2
    a={r['interwal']:r for r in rows}
    assert a['1h']['ocena_diagnostyczna']=='BLOKADA_WG_QC' and a['1h']['brakuje_swiec']==23
    assert a['15m']['ocena_diagnostyczna']=='ODRZUCENIE_BEZ_BLOKADY'
    assert qc==before and raport_qc_przyczyn_v176([],[],[])==[]
    return 'PASS (przyczyny, licznik powtorzen, blokada vs ostrzezenie, bez mutacji)'



def raport_qc_zmiany_v177(current, previous, status):
    """Raport wszystkich zmian, w tym ustapien; brak previous != ustapienie."""
    rows=[]
    if status != 'POROWNANO' or not isinstance(previous, dict):
        return rows
    for key in sorted(set(current) | set(previous)):
        before=previous.get(key)
        after=current.get(key)
        if not isinstance(before, dict) or not isinstance(after, dict):
            rows.append({'para': key, 'zmiana': 'NOWA_PARA' if before is None else 'BRAK_W_BIEZACYM_SKANIE',
                         'odrzucone_przed': before.get('odrzucone') if isinstance(before,dict) else None,
                         'odrzucone_teraz': after.get('odrzucone') if isinstance(after,dict) else None,
                         'blokada_przed': before.get('blokuje_analize') if isinstance(before,dict) else None,
                         'blokada_teraz': after.get('blokuje_analize') if isinstance(after,dict) else None})
            continue
        ob=bool(before.get('odrzucone',False)); nb=bool(after.get('odrzucone',False))
        bb=bool(before.get('blokuje_analize',False)); ab=bool(after.get('blokuje_analize',False))
        if ob and not nb: label='ODRZUCENIA_USTAPILY'
        elif not ob and nb: label='NOWE_ODRZUCENIA'
        elif ob and nb: label='ODRZUCENIA_TRWAJA'
        else: label='BEZ_ODRZUCEN'
        if bb != ab: label += '|BLOKADA_WLACZONA' if ab else '|BLOKADA_ZDJETA'
        rows.append({'para':key,'zmiana':label,'odrzucone_przed':ob,'odrzucone_teraz':nb,
                     'blokada_przed':bb,'blokada_teraz':ab})
    return rows


def raport_qc_udzial_v177(qc_rows, summary_rows, live_rows, top_rows):
    """Obecnosc w populacjach raportow, NIE atrybucja wyniku czy przyczynowosc."""
    def symbols(rows):
        return {str(r.get('symbol','')) for r in (rows or []) if isinstance(r,dict) and r.get('symbol')}
    scanned=symbols(summary_rows); analyzed=symbols(live_rows); selected=symbols(top_rows)
    rows=[]
    for q in qc_rows:
        sym=str(q['symbol'])
        rows.append({**q, 'w_summary':sym in scanned, 'w_live_setups':sym in analyzed,
                     'w_top_setups':sym in selected,
                     'wpływ_na_backtest':'NIEUSTALONY_BEZ_SLADU_ZDARZEN',
                     'uwaga':'Obecnosc symbolu nie dowodzi uzycia wadliwego interwalu ani wplywu na PnL'})
    return rows


def test_v177():
    old={'X|1h':{'odrzucone':True,'blokuje_analize':True},
         'Y|4h':{'odrzucone':True,'blokuje_analize':False}}
    now={'X|1h':{'odrzucone':False,'blokuje_analize':False},
         'Z|1h':{'odrzucone':True,'blokuje_analize':True}}
    assert raport_qc_zmiany_v177(now,old,'BEZ_DYSKU')==[]
    d=raport_qc_zmiany_v177(now,old,'POROWNANO')
    assert len(d)==3 and 'ODRZUCENIA_USTAPILY' in d[0]['zmiana']
    assert any(r['zmiana']=='BRAK_W_BIEZACYM_SKANIE' for r in d)
    a=raport_qc_udzial_v177([{'symbol':'X','interwal':'1h'}], [{'symbol':'X'}], [], [])
    assert a[0]['w_summary'] and not a[0]['w_live_setups'] and a[0]['wpływ_na_backtest'].startswith('NIEUSTALONY')
    return 'PASS (ustapienie QC, znikajace pary, brak poprzedniego snapshotu, bez falszywej atrybucji PnL)'

def raport_qc_skrot_v178(rows):
    """Tylko niezerowe zmiany; BEZ_ODRZUCEN odfiltrowane, brak skanu zachowany."""
    return [dict(r) for r in (rows or []) if str(r.get('zmiana','')) != 'BEZ_ODRZUCEN']


def raport_qc_wejscia_v178(qc_rows, kontrola_rows, summary_rows, live_rows, top_rows):
    """Dowod dostepnosci wejscia, NIE dowod wykonania swiec w silniku lub wplywu na R."""
    def syms(rows):
        return {str(r.get('symbol')) for r in (rows or []) if isinstance(r,dict) and r.get('symbol')}
    idx={(str(r.get('symbol')),str(r.get('interwal'))):r for r in (kontrola_rows or []) if isinstance(r,dict)}
    summary_syms=syms(summary_rows); live_syms=syms(live_rows); top_syms=syms(top_rows)
    out=[]
    for row in (qc_rows or []):
        sym=str(row.get('symbol','')); tf=str(row.get('interwal',''))
        source=idx.get((sym,tf))
        out.append({'symbol':sym,'interwal':tf,
          'dowod_wejscia':'QC_ZAREJESTROWANY' if source is not None else 'BRAK_KONTROLI_DLA_PARY',
          'swiece_dostepne':source.get('swiece_dostepne','') if source else '',
          'swiece_odrzucone':source.get('swiece_odrzucone','') if source else '',
          'blokuje_analize':source.get('blokuje_analize','') if source else '',
          'w_summary':sym in summary_syms,'w_live_setups':sym in live_syms,
          'w_top_setups':sym in top_syms,
          'czy_interwal_faktycznie_uzyty':'NIEPOTWIERDZONE_BRAK_SLADU_WYWOLAN',
          'wplyw_na_wynik_r':'NIEUSTALONY_BEZ_REPLAY_ZDARZEN'})
    return out


def test_v178():
    a=raport_qc_skrot_v178([{'zmiana':'BEZ_ODRZUCEN'},
        {'zmiana':'ODRZUCENIA_TRWAJA','para':'X|1h'},
        {'zmiana':'BRAK_W_BIEZACYM_SKANIE','para':'Y|4h'}])
    assert len(a)==2 and a[1]['zmiana']=='BRAK_W_BIEZACYM_SKANIE'
    b=raport_qc_wejscia_v178([{'symbol':'X','interwal':'1h'}],
       [{'symbol':'X','interwal':'1h','swiece_dostepne':180,'swiece_odrzucone':4,'blokuje_analize':False}],
       [{'symbol':'X'}],[],[])
    assert b[0]['swiece_odrzucone']==4 and b[0]['w_summary']
    assert b[0]['czy_interwal_faktycznie_uzyty'].startswith('NIEPOTWIERDZONE')
    assert raport_qc_wejscia_v178([],[],[],[],[])==[]
    return 'PASS (delta tylko istotne, pary nieobecne, metadane interwalu, bez falszywej atrybucji)'

# V6.179 — audyt wyłącznie diagnostyczny, bez ingerencji w portfel OOS.
def _v179_event_snapshot(csv_path):
    """Utrwala rozliczenia z faktycznego CSV, nie rekonstruuje nieznanych cen wejscia."""
    import csv
    if not Path(csv_path).is_file():
        return None
    with open(csv_path, newline='', encoding='utf-8-sig') as fh:
        src = list(csv.DictReader(fh))
    counts, out = {}, {}
    for r in src:
        base = '|'.join(str(r.get(k, '')) for k in
                        ('rynek_analizy','variant','fold','abc_id','snapshot_utc','decyzja'))
        ordinal = counts.get(base, 0)
        counts[base] = ordinal + 1
        # Licznik jest jawny: powtorzenia nie sa cicho nadpisywane.
        key = base + '|wystapienie=' + str(ordinal)
        out[key] = {k: str(r.get(k, '')) for k in
                    ('rynek_analizy','variant','fold','abc_id','snapshot_utc',
                     'decyzja','exit_utc','koszt_r','r_netto','aktywne_przed',
                     'aktywne_po','wybrany','status_zrodla','pochodzenie')}
    return out


def _v179_event_delta(before, after):
    cols = ('exit_utc','koszt_r','r_netto','aktywne_przed','aktywne_po',
            'wybrany','status_zrodla','pochodzenie')
    rows = []
    for key in sorted(set(before) | set(after)):
        old, new = before.get(key), after.get(key)
        if old is None:
            status, diff = 'NOWE_ZDARZENIE', 'NOWE'
        elif new is None:
            status, diff = 'BRAK_W_NOWYM_PRZEBIEGU', 'BRAK'
        else:
            changed = [k for k in cols if old.get(k,'') != new.get(k,'')]
            if not changed:
                continue
            status, diff = 'ZMIENIONE_ZDARZENIE', ';'.join(changed)
        rows.append({'klucz_zdarzenia':key,'status':status,'zmienione_pola':diff,
                     'r_netto_przed':old.get('r_netto','') if old else '',
                     'r_netto_teraz':new.get('r_netto','') if new else '',
                     'koszt_r_przed':old.get('koszt_r','') if old else '',
                     'koszt_r_teraz':new.get('koszt_r','') if new else '',
                     'exit_utc_przed':old.get('exit_utc','') if old else '',
                     'exit_utc_teraz':new.get('exit_utc','') if new else '',
                     'sloty_przed':old.get('aktywne_po','') if old else '',
                     'sloty_teraz':new.get('aktywne_po','') if new else '',
                     'uwaga':'Porownanie raportow; brak cen entry/SL w CSV zdarzen'})
    return rows



# V6.180 — rygorystyczna porownywalnosc: identyczne archiwa OHLCV, sklad i konfiguracja.
def _v180_fingerprint(out_dir, drive_dir):
    import hashlib, csv
    out_dir, drive_dir = Path(out_dir), Path(drive_dir)
    report = {'schema':2, 'status':'NIEPOTWIERDZONE', 'algorytm':'sha256',
              'zakres':'caly zestaw symboli summary.csv i archiwa candles_v63',
              'ograniczenie':'Zgodnosc plikow z Drive nie dowodzi identycznosci buforow OHLCV w pamieci podczas obliczen.'}
    summary = out_dir/'summary.csv'
    if not summary.is_file():
        report['powod'] = 'BRAK_SUMMARY'; return report
    try:
        with summary.open(newline='',encoding='utf-8-sig') as fh:
            symbols=sorted(set(str(r.get('symbol','')).strip() for r in csv.DictReader(fh) if r.get('symbol')))
        if not symbols:
            report['powod']='PUSTA_POPULACJA'; return report
        import re
        if any(not re.fullmatch(r'[A-Za-z0-9_]+',sym) for sym in symbols):
            report['powod']='NIEPOPRAWNY_SYMBOL'; return report
        report['symbol_count']=len(symbols)
        report['symbols']=symbols
        report['population_sha256']=hashlib.sha256(('\n'.join(symbols)).encode()).hexdigest()
        config_keys=sorted(k for k,v in globals().items() if
            (k.startswith('ELLIOTT_') or k.startswith('HISTORICAL_SLIP_') or
             k in ('TOP_N','TRADFI_TOP','MAX_OPEN_POSITIONS','WORKERS',
                   'MAX_COST_R','EXT_CHASE_MAX_ATR','SHORT_SQUEEZE_EXT_ATR',
                   'SETUP_MIN_RR','SETUP_WAIT_H','GATE_FROM_UTC','GATE_MIN_TRADES',
                   'GATE_MIN_AVG_R','GATE_MIN_T','BT_LOOKBACK_H','BT_STEP_H',
                   'BT_MAX_HOLD_H','HIST_DAYS','KAPITAL_USDT',
                   'RYZYKO_NA_TRANSAKCJE_PCT','PREFEROWANA_DZWIGNIA',
                   'LEV_LIQ_SL_MULT','LEV_MMR','MAX_DEPOZYT_PCT'))
            and isinstance(v,(str,int,float,bool,type(None))))
        config={k:globals()[k] for k in config_keys}
        report['config_sha256']=hashlib.sha256(json.dumps(config,sort_keys=True,allow_nan=False).encode()).hexdigest()
        report['config_keys']=config_keys
        report['config_values']=config
        archive=drive_dir/'candles_v63'
        digest=hashlib.sha256()
        absent=[]
        file_hashes={}
        for sym in symbols:
            f=archive/(sym+'.npz')
            if not f.is_file():
                absent.append(sym);continue
            h=hashlib.sha256()
            with f.open('rb') as fh:
                for chunk in iter(lambda:fh.read(1024*1024),b''):
                    h.update(chunk)
            file_hashes[sym]=h.hexdigest()
            digest.update(sym.encode()+b'\0'+h.digest())
        report['archive_files']=file_hashes
        report['archiwa_znalezione']=len(symbols)-len(absent)
        report['archiwa_brakujace']=absent[:20]
        if absent:
            report['powod']='BRAK_ARCHIWOW_DLA_CZESCI_POPULACJI';return report
        report['ohlcv_archive_sha256']=digest.hexdigest()
        report['status']='HASH_PLIKOW_GOTOWY'
        return report
    except (OSError, ValueError, TypeError) as exc:
        report['powod']='BLAD_ODCZYTU: '+str(exc)[:160]
        return report


def _v180_comparability(before, current):
    if not isinstance(before,dict) or before.get('schema')!=2:
        return 'NIEPOROWNYWALNE_BRAK_FINGERPRINTU_POPRZEDNIO',[]
    if before.get('status')!='HASH_PLIKOW_GOTOWY' or current.get('status')!='HASH_PLIKOW_GOTOWY':
        return 'NIEZWERYFIKOWANE_NIEPELNE_DANE',[]
    fields=('population_sha256','config_sha256','ohlcv_archive_sha256')
    changed=[k for k in fields if not before.get(k) or before.get(k)!=current.get(k)]
    if changed: return 'NIEPOROWNYWALNE_ZMIENIONE_DANE_LUB_KONFIG', changed
    return 'ZGODNE_PLIKI_KONFIG_I_POPULACJA', []


def test_v180():
    import tempfile
    with tempfile.TemporaryDirectory() as d:
        root=Path(d); out=root/'out'; out.mkdir(); drive=root/'drive'; (drive/'candles_v63').mkdir(parents=True)
        (out/'summary.csv').write_text('symbol\nAAAUSDT\n',encoding='utf-8')
        (drive/'candles_v63'/'AAAUSDT.npz').write_bytes(b'ohlcv')
        a=_v180_fingerprint(out,drive)
        assert a['status']=='HASH_PLIKOW_GOTOWY'
        assert _v180_comparability(a,a)[0]=='ZGODNE_PLIKI_KONFIG_I_POPULACJA'
        assert _v180_comparability({'schema':1},a)[0].startswith('NIEPOROWNYWALNE')
        (drive/'candles_v63'/'AAAUSDT.npz').write_bytes(b'ohlcv_CHANGED')
        b=_v180_fingerprint(out,drive)
        assert 'ohlcv_archive_sha256' in _v180_comparability(a,b)[1]
        (drive/'candles_v63'/'AAAUSDT.npz').unlink()
        assert _v180_fingerprint(out,drive)['status']=='NIEPOTWIERDZONE'
    return 'PASS (populacja, konfiguracja, OHLCV, legacy i brak archiwum fail-closed)'



# V6.181: niezalezny audyt plikow, bez ingerencji w logike strategii.
def _v181_archive_inventory(drive_dir):
    import hashlib
    root=Path(drive_dir)/'candles_v63'
    if not root.is_dir():
        return {'status':'BRAK_ARCHIWUM','files':{}}
    files={}
    try:
        for p in sorted(root.glob('*.npz')):
            h=hashlib.sha256()
            with p.open('rb') as fh:
                for block in iter(lambda:fh.read(1024*1024),b''):
                    h.update(block)
            files[p.stem]=h.hexdigest()
        return {'status':'OK','files':files}
    except OSError as exc:
        return {'status':'BLAD_ODCZYTU:'+str(exc)[:120],'files':{}}


def _v181_diagnostic(old_fp,new_fp,pre_inventory,post_inventory):
    import hashlib
    before=(old_fp or {}) if isinstance(old_fp,dict) else {}
    after=(new_fp or {}) if isinstance(new_fp,dict) else {}
    old_symbols=set(before.get('symbols',[]) or [])
    new_symbols=set(after.get('symbols',[]) or [])
    symbol_details_available='symbols' in before and 'symbols' in after
    old_files=before.get('archive_files',{}) if isinstance(before.get('archive_files'),dict) else {}
    new_files=after.get('archive_files',{}) if isinstance(after.get('archive_files'),dict) else {}
    file_details_available='archive_files' in before and 'archive_files' in after
    changed_files=[k for k in sorted(set(old_files)&set(new_files)) if old_files[k]!=new_files[k]]
    pre=pre_inventory.get('files',{}) if isinstance(pre_inventory,dict) else {}
    post=post_inventory.get('files',{}) if isinstance(post_inventory,dict) else {}
    prepost_ok=pre_inventory.get('status')=='OK' and post_inventory.get('status')=='OK'
    changed_during=[x for x in sorted(set(pre)&set(post)) if pre[x]!=post[x]] if prepost_ok else []
    return {
        'status':'DIAGNOSTYKA_PLIKOW_NIE_BUFOROW_AS_OF',
        'porownanie_populacji':{'status':'POROWNANO' if symbol_details_available else 'BRAK_DANYCH_POPRZEDNIEGO_SNAPSHOTU',
             'dodane':sorted(new_symbols-old_symbols) if symbol_details_available else [],
             'usuniete':sorted(old_symbols-new_symbols) if symbol_details_available else []},
        'archiwa_miedzy_przebiegami':{'status':'POROWNANO' if file_details_available else 'BRAK_DANYCH_POPRZEDNIEGO_SNAPSHOTU',
             'zmienione_symbole':changed_files if file_details_available else [],
             'dodane':sorted(set(new_files)-set(old_files)) if file_details_available else [],
             'usuniete':sorted(set(old_files)-set(new_files)) if file_details_available else []},
        'archiwa_przed_po_odswiezeniu':{'status':'POROWNANO' if prepost_ok else 'NIEZWERYFIKOWANE',
             'zmodyfikowane':changed_during,'dodane':sorted(set(post)-set(pre)) if prepost_ok else [],
             'usuniete':sorted(set(pre)-set(post)) if prepost_ok else [],
             'uwaga':'Zmiana hash pliku NPZ nie rozroznia dopisania nowej swiecy od korekty starej; wymaga porownania timestampow i OHLCV.'},
        'konfiguracja':{'zmienione_klucze':[k for k in sorted(set(before.get('config_values',{}))|set(after.get('config_values',{})))
            if before.get('config_values',{}).get(k)!=after.get('config_values',{}).get(k)]
            if 'config_values' in before and 'config_values' in after else [],
            'status':'POROWNANO' if 'config_values' in before and 'config_values' in after else 'BRAK_DANYCH_POPRZEDNIO'},
        'ograniczenie':'Hashe archiwow identyfikuja pliki, nie dowodza wykorzystania danych as-of ani przyczyny zmiany wyniku.'}


def test_v181():
    import tempfile
    with tempfile.TemporaryDirectory() as d:
        root=Path(d); (root/'candles_v63').mkdir(); (root/'candles_v63'/'AAA.npz').write_bytes(b'a')
        a=_v181_archive_inventory(root)
        assert a['status']=='OK' and len(a['files'])==1
        (root/'candles_v63'/'AAA.npz').write_bytes(b'b')
        b=_v181_archive_inventory(root)
        x=_v181_diagnostic({'symbols':['AAA'],'archive_files':{'AAA':'a'},'config_values':{'X':1}},
           {'symbols':['BBB'],'archive_files':{'AAA':'b'},'config_values':{'X':2}},a,b)
        assert x['porownanie_populacji']['dodane']==['BBB']
        assert x['archiwa_miedzy_przebiegami']['zmienione_symbole']==['AAA']
        assert x['archiwa_przed_po_odswiezeniu']['zmodyfikowane']==['AAA']
        assert x['konfiguracja']['zmienione_klucze']==['X']
        assert _v181_diagnostic({}, {}, a,b)['porownanie_populacji']['status'].startswith('BRAK')
    return 'PASS (symbole, hashe archiwow, pre/post, konfiguracja, legacy fail-closed)'



# V6.182: odcisk rekordu po czasie (pelny OHLCV i pola dodatkowe), bez zatrzymywania duzych kopii NPZ.
def _v182_candle_snapshot(drive_dir):
    import hashlib
    root=Path(drive_dir)/'candles_v63'
    result={'status':'OK','series':{},'errors':{}}
    if not root.is_dir():
        result['status']='BRAK_ARCHIWUM';return result
    for p in sorted(root.glob('*.npz')):
        if p.stem.endswith('.tmp'):
            continue
        try:
            with np.load(p,allow_pickle=False) as z:
                per_tf={}
                for tf in z.files:
                    if tf not in INTERVAL_MS: continue
                    data=np.asarray(z[tf])
                    if data.ndim!=2 or data.shape[1]<=T:
                        raise ValueError('NIEPRAWIDLOWY_KSZTALT_'+tf)
                    stamps=data[:,T]
                    if not np.all(np.isfinite(stamps)):
                        raise ValueError('NIEPOPRAWNY_TIMESTAMP_'+tf)
                    if len(stamps)!=len(np.unique(stamps)):
                        raise ValueError('DUPLIKAT_TIMESTAMP_'+tf)
                    records={}
                    for row in data:
                        ts=int(row[T]);
                        if float(ts)!=float(row[T]): raise ValueError('NIECALKOWITY_TIMESTAMP_'+tf)
                        # O,H,L,C,V,QV,TB,T w oryginalnym ukladzie; porownujemy wszystkie pola.
                        b=np.asarray(row,dtype=np.float64).astype('<f8',copy=False).tobytes()
                        records[ts]=hashlib.blake2b(b,digest_size=12).hexdigest()
                    per_tf[tf]=records
                result['series'][p.stem]=per_tf
        except (OSError,ValueError,TypeError,KeyError) as exc:
            result['errors'][p.stem]=str(exc)[:150]
    if result['errors']:
        result['status']='CZESC_ARCHIWOW_NIEZWERYFIKOWANA'
    return result


def _v182_candle_diff(before, after):
    if before.get('status')!='OK' or after.get('status')!='OK':
        return [{'symbol':'','interwal':'','status':'NIEZWERYFIKOWANE',
                 'powod':'BLAD_LUB_BRAK_ARCHIWUM','nowe_na_koncu':0,
                 'uzupelnione_luki':0,'korekty_istniejacych':0,'usuniete':0}]
    rows=[]
    old=before.get('series',{}); new=after.get('series',{})
    for sym in sorted(set(old)|set(new)):
        for tf in sorted(set(old.get(sym,{}))|set(new.get(sym,{}))):
            a=old.get(sym,{}).get(tf,{}); b=new.get(sym,{}).get(tf,{})
            common=set(a)&set(b); added=set(b)-set(a); deleted=set(a)-set(b)
            old_max=max(a) if a else None
            append=sum(t>old_max for t in added) if old_max is not None else 0
            gap=len(added)-append if old_max is not None else 0
            changed=sum(a[t]!=b[t] for t in common)
            if not (added or deleted or changed):continue
            rows.append({'symbol':sym,'interwal':tf,
                         'status':'POROWNANO' if a and b else 'NOWA_LUB_USUNIETA_SERIA',
                         'powod':'ODCISK_CALY_REKORD',
                         'nowe_na_koncu':append if a else len(added),
                         'uzupelnione_luki':gap,'korekty_istniejacych':changed,
                         'usuniete':len(deleted),'przed':len(a),'po':len(b),
                         'uwaga':'Nowe na koncu, gdy timestamp > max starej serii; usuniecia moga wynikac z retencji.'})
    return rows


def test_v182():
    import tempfile
    with tempfile.TemporaryDirectory() as d:
        root=Path(d); folder=root/'candles_v63';folder.mkdir()
        def rec(t,c):return [c,c+1,c-1,c,10,11,1,t]
        np.savez_compressed(folder/'AA.npz',**{'5m':np.array([rec(100,2),rec(200,3)],dtype=float)})
        a=_v182_candle_snapshot(root); assert a['status']=='OK'
        np.savez_compressed(folder/'AA.npz',**{'5m':np.array([rec(100,2.5),rec(150,4),rec(200,3),rec(300,5)],dtype=float)})
        b=_v182_candle_snapshot(root); rows=_v182_candle_diff(a,b)
        assert len(rows)==1 and rows[0]['korekty_istniejacych']==1
        assert rows[0]['nowe_na_koncu']==1 and rows[0]['uzupelnione_luki']==1
        assert _v182_candle_diff(b,b)==[]
        np.savez_compressed(folder/'AA.npz',**{'5m':np.array([rec(100,2),rec(100,2)],dtype=float)})
        assert _v182_candle_snapshot(root)['status']!='OK'
        assert _v182_candle_diff(a,_v182_candle_snapshot(root))[0]['status']=='NIEZWERYFIKOWANE'
    return 'PASS (append, uzupelnienie luki, korekta, duplikaty fail-closed)'

def _v179_audit_run(out_dir, drive_dir, drive_ok, replay, fetch_stats,
                    history_repairs, funding_reasons, pre_inventory=None):
    """Bledy snapshotu nie zmieniaja handlu; bledna baza daje brak porownania."""
    import csv
    out_dir = Path(out_dir)
    now = _v179_event_snapshot(out_dir/'elliott_abc_portfel_ciagly_zdarzenia_v166.csv')
    old_path = Path(drive_dir)/'elliott_abc_zdarzenia_snapshot_v179.json'
    old, state = {}, 'BRAK_POPRZEDNIEGO_SNAPSHOTU'
    old_fingerprint = None
    fingerprint = _v180_fingerprint(out_dir,drive_dir) if drive_ok and not replay else {'schema':2,'status':'NIEPOTWIERDZONE','powod':'REPLAY_LUB_BRAK_DRIVE'}
    if replay or not drive_ok:
        state = 'REPLAY_LUB_BRAK_DRIVE_BEZ_POROWNANIA'
    elif old_path.is_file():
        try:
            raw = json.loads(old_path.read_text(encoding='utf-8'))
            if raw.get('schema') == 1 and isinstance(raw.get('events'), dict):
                old = raw['events']; state = 'POROWNANO'; old_fingerprint = raw.get('fingerprint')
            else:
                state = 'NIEZGODNY_FORMAT_BAZY'
        except (OSError, ValueError, TypeError) as exc:
            state = 'BLAD_ODCZYTU_BAZY: '+str(exc)[:100]
    if now is None:
        state = 'BRAK_CSV_ZDARZEN'
    comparability, changed_fields = _v180_comparability(old_fingerprint, fingerprint)
    detail181=_v181_diagnostic(old_fingerprint,fingerprint,pre_inventory or {},_v181_archive_inventory(drive_dir))
    save_json('audyt_danych_v181.json',detail181,pretty=True)
    differences = _v179_event_delta(old,now) if now is not None and state == 'POROWNANO' else []
    cols = ['klucz_zdarzenia','status','zmienione_pola','r_netto_przed',
            'r_netto_teraz','koszt_r_przed','koszt_r_teraz','exit_utc_przed',
            'exit_utc_teraz','sloty_przed','sloty_teraz','uwaga']
    write_csv('elliott_oos_roznice_zdarzen_v179.csv', differences, cols, cols=cols)
    cache_rows = [{'zrodlo':'KLINES','powod':str(k),'liczba':int(v),
                   'poziom':'ZBIORCZY_BEZ_SYMBOLU'} for k,v in sorted(fetch_stats.items())]
    for (symbol,tf), info in sorted(history_repairs.items()):
        if info.get('powod') != 'archiwum_aktualne':
            cache_rows.append({'zrodlo':'KLINES','symbol':symbol,'interwal':tf,
                               'powod':str(info.get('powod','NIEZNANY')),
                               'liczba':1,'poziom':'SYMBOL_INTERWAL',
                               'zadano':info.get('zadano',''),
                               'otrzymano':info.get('otrzymano',''),
                               'blad':info.get('blad','')})
    for r in funding_reasons or []:
        cache_rows.append({'zrodlo':'FUNDING','symbol':r.get('symbol',''),
                           'powod':r.get('powod',''), 'liczba':1,
                           'poziom':'SYMBOL','szczegoly':r.get('powod_szczegolowy',''),
                           'blad':r.get('blad','')})
    cache_cols = ['zrodlo','symbol','interwal','powod','liczba','poziom',
                  'zadano','otrzymano','szczegoly','blad']
    write_csv('cache_przyczyny_v179.csv',cache_rows,cache_cols,cols=cache_cols)
    result = {'status_porownania':state,'zdarzenia_teraz':len(now or {}),
              'zdarzenia_poprzednio':len(old) if state=='POROWNANO' else None,
              'roznice':len(differences),'uwaga':'Wyniki nie sa automatycznie uznawane za porownywalne przy zmianie populacji lub danych. Brak bezposrednich cen entry/SL.',
              'cache_rekordy':len(cache_rows),
              'porownywalnosc_v180':comparability if state=='POROWNANO' else 'NIEZWERYFIKOWANE_BRAK_POROWNANIA',
              'zmienione_fingerprinty':changed_fields, 'fingerprint_v180':fingerprint, 'diagnostyka_v181':'audyt_danych_v181.json',
              'uwaga_v180':'Delta zdarzen jest diagnostyczna; status POROWNANO nie oznacza porownywalnych danych. Hash archiwow nie dowodzi as-of buforow.'}
    save_json('audyt_przebiegow_v179.json',result,pretty=True)
    if now is not None and drive_ok and not replay:
        try:
            zapisz_atomowo(old_path, lambda temp: Path(temp).write_text(
                json.dumps({'schema':1,'saved_utc':utc_now(),'events':now,'fingerprint':fingerprint},
                           ensure_ascii=False,allow_nan=False), encoding='utf-8'), kopia=True)
        except (OSError,ValueError,TypeError) as exc:
            result['blad_zapisu_snapshotu'] = str(exc)[:140]
            save_json('audyt_przebiegow_v179.json',result,pretty=True)
    return result


def test_v179():
    import tempfile
    with tempfile.TemporaryDirectory() as d:
        p = Path(d)/'e.csv'
        p.write_text('rynek_analizy,variant,fold,abc_id,snapshot_utc,decyzja,r_netto,koszt_r,exit_utc\n'
                     'CRYPTO,F,1,a,T,W,1,0.1,X\nCRYPTO,F,1,a,T,W,1,0.1,X\n',encoding='utf-8')
        a = _v179_event_snapshot(p)
        assert len(a)==2
        b = {k:dict(v) for k,v in a.items()}
        b[next(iter(b))]['r_netto']='-1'
        assert len(_v179_event_delta(a,b))==1
        assert len(_v179_event_delta(a,{}))==2
        assert len(_v179_event_delta(a,a))==0
    return 'PASS (unikaty zdarzen, zmiana R, znikniecie, brak zmian)'


def main():
    print('Test V6.182:', test_v182())
    print('Test V6.181:', test_v181())
    print('Test V6.180:', test_v180())
    print('Test V6.179:', test_v179())
    print('Test V6.178:', test_v178())
    print('Test V6.177:', test_v177())
    print('Test V6.176:', test_v176())
    print('Test launchera V6.175:', test_v175())
    print('Test V6.174:', test_v174())
    print('Test V6.173:', test_v173())
    print('Test V6.172:', test_v172())
    print('Test schematu V6.170:', test_v170())
    print('Test eksportu V6.171:', test_v171())
    print('Test Elliott V6.165:', test_v165())
    print('Test Elliott V6.166:', test_v166())
    print('Test Elliott V6.167:', test_v167())
    print('Test Elliott V6.164:', test_v164())
    print('Test Elliott V6.163:', test_v163())
    print('Test Elliott V6.162:', test_v162())
    print('Test Elliott V6.161:', test_v161())
    print('Test Elliott V6.160:', test_v160())
    print('Test Elliott backtest V6.159:', test_elliott_bt_v159())
    print('Test Elliott V6.157:', test_elliott_wlasne_v156())
    print('Test Elliott V6.158:', test_elliott_v158())
    print('Test kosztow V6.151:', test_cost_comparison_v151())
    print('Test kosztow V6.152:', test_cost_legs_v152())
    print('Test kosztow V6.153:', test_koszt_replay_v153())
    print('Test bramki kosztow V6.155:', test_koszt_gate_v155())
    print('Test V6.149:', test_v149())
    print('Test V6.148:', test_v148())
    t_start = time.time()
    _v181_before_archive = _v181_archive_inventory(DRIVE_DIR)
    # V6.183: snapshot OHLCV dopiero po ustaleniu stanu Drive oraz trybu replay.
    if TEST_BRAMKI:
        print('Test cache fundingu V6.138:', test_funding_cache_v138())
        print('Test postepu fundingu V6.139:', test_funding_progress_v139())
        print('Test opoznienia fundingu V6.140:', test_funding_opoznienie_v140())
        print('Test chronologii fundingu V6.141:', test_funding_chronologia_v141())
        print('Test rozkladu fundingu V6.142:', test_funding_odstepy_v142())
        print('Test ostatnich cykli V6.143:', test_funding_ostatnie_cykle_v143())
        print('Test odstepow V6.144:', test_funding_v144())
        print('Test rezimu V6.145:', test_funding_v145())
        print('Test pelnej serii fundingu V6.146:', test_v146())
        print('Test diagnostyki V6.147:', test_v147())
        print('Test hash V6.145:', test_runtime_identity_v145())
        print('Test identyfikacji pliku V6.142:', test_runtime_identity_v142())
        print('Test bramki:', test_bramki())          # V6.22
        print('Test dziennika PROBA:', test_proba_rekordy())   # V6.25
        print('Test hipotezy:', test_hipotezy())               # V6.38
        print('Test limitu prob:', test_proba_limit())
        print('Test wieku prob:', test_proba_wiek())           # V6.41
        print('Test ekspozycji:', test_ekspozycji())           # V6.46
        print('Test ekspozycji (konfiguracja):', test_ekspozycji_konfig())   # V6.49
        print('Test dziennika PROBA (bledy):', test_proba_dziennik())        # V6.49
        print('Test okna testu 30 dni:', test_hist_okno())               # V6.51
        print('Test daty regul:', test_reguly_od())                      # V6.55
        print('Test luki MARKET:', test_market_luka())                  # V6.58
        print('Test zapisu na Drive:', test_zapisu())                   # V6.59
        print('Test dziennikow i pustych CSV:', test_dziennikow())      # V6.66
        print('Test wagi zdjec w bramce:', test_waga_zdjec())            # V6.72
        print('Test Elliott:', test_elliott_structure())                  # V6.75
        print('Test raportow Elliott:', test_elliott_raporty())           # V6.76
        print('Test audytu przyczyn Elliott V6.129:', test_audyt_przyczyn_v129())
        print('Test sladu rozliczen V6.130:', test_audyt_v130())
        print('Test audytu V6.131:', test_audyt_v131())
        print('Test audytu V6.132:', test_audyt_v132())
        print('Test audytu V6.134:', test_audyt_v134())
        print('Test audytu V6.135:', test_audyt_v135())
        print('Test slippage OOS V6.136:', test_historical_slippage_v136())
        print('Test scenariuszy V6.137:', test_sensitivity_oos_v137())
        print('Test zdarzen i dziennego OOS:', test_elliott_v681())
        print('Test odpornosci A/B:', test_elliott_v682())
        print('Test wszystkich dni A/B:', test_elliott_v683())
        print('Test diagnostyki OOS V6.84:', test_elliott_v684())
        print('Test diagnostyki OOS V6.85:', test_elliott_v685())
        print('Test przyrostu OOS V6.86:', test_elliott_v686())       # V6.81
        print('Test PORTFOLIO_THREE_WAY:', test_portfolio_three_way())  # V6.89
        print('Test stabilnosci THREE_WAY:', test_portfolio_three_way_stability())
        print('Test V6.91:', test_portfolio_v691())  # V6.90
        print('Test MARKET_PIERWSZE:', test_market_ab())                  # V6.87
        print('Test TradFi 24h:', test_tradfi_24h())                      # V6.88
        print('Test TradFi bez blokad:', test_tradfi_bez_blokad())        # V6.92
        assert od_strona({'CONT_SWEEP.v4': '2026-10-15T08:00'}, 'CONT_SWEEP.v4', 'LONG') == '2026-10-26T08:00'    # V6.71
        assert od_strona({'CONT_SWEEP.v4': '2026-10-15T08:00'}, 'CONT_SWEEP.v4', 'SHORT') == '2026-10-26T08:00'   # V6.71
        assert od_strona({'CONT_SWEEP.v4.M': '2026-10-15T08:00'}, 'CONT_SWEEP.v4.M', 'SHORT') == '2026-10-26T08:00'
        assert od_strona({'PULLBACK.v4': '2026-10-15T08:00'}, 'PULLBACK.v4', 'LONG') == '2026-10-15T08:00'
        assert typ_wylaczony('PULLBACK', 'LONG', 'MARKET') and typ_wylaczony('CONT_SWEEP', 'LONG', 'LIMIT')   # V6.71
        assert not typ_wylaczony('CONT_SWEEP', 'LONG', 'MARKET') and not typ_wylaczony('CONT_SWEEP', 'SHORT', 'LIMIT')
        assert not typ_wylaczony('ODBICIE', 'LONG', 'MARKET')
    if OUT.exists():
        shutil.rmtree(OUT)
    OUT.mkdir(parents=True)
    replay = bool(REPLAY_ZIP)
    drive_ok = mount_drive() if (USE_DRIVE and IN_COLAB and not replay) else False
    _v182_before_candles = _v182_candle_snapshot(DRIVE_DIR) if drive_ok and not replay else {'status':'BRAK_ARCHIWUM'}
    bez_dysku = not drive_ok and not replay            # V6.42
    if bez_dysku:
        baner_brak_dysku()
        (OUT / 'BRAK_DYSKU.txt').write_text(BRAK_DYSKU_TXT + '\n')
    positions = load_positions(drive_ok)
    ALWAYS_INCLUDE.extend(p['symbol'] for p in positions)
    pos_rows = []
    _czasy.clear()
    _api['n'] = 0
    _api_by_endpoint.clear()
    _io_outcomes.clear()
    _fetch_stats.clear()
    _funding_io.clear()
    _bt_cache_io.clear()
    _bt_cache_io.update(status='nie_uzyto', odczyt_s=0.0, zapis_s=0.0, rozmiar_b=0, wpisy=0)
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
        load_short_history(drive_ok)
        # Archiwum zostalo wczytane przed load_short_history: uzupelnij dowody.
        for sym, intervals in known.items():
            for tf, info in list(intervals.items()):
                if len(info) >= 3:
                    intervals[tf] = (*info[:3], _short_history_verified.get((sym, tf)) == info[0])
        fund_cache = load_funding_cache(drive_ok)
        _funding_io['aktualne'] = sum(funding_aktualny(v, int(time.time()*1000)+_clock['offset_ms']) for v in fund_cache.values())
        _funding_io['brak_w_cache'] = 0
        _funding_io['cache_nieaktualny'] = 0
        _funding_io['cache_poprawny'] = 0
        print(f"  funding cache: {_funding_io['odczyt']}, wpisy {_funding_io['wpisy']}, aktualne {_funding_io['aktualne']}")
        with etap('pobieranie API'):
            coins, now_ms = collect(known, fund_cache)
        # V6.128: diagnozuj przyczyne odswiezenia per symbol, bez zmiany strategii pobierania.
        funding_reasons=[]
        for sym in sorted(coins):
            hist=fund_cache.get(sym)
            if hist is None:
                reason='BRAK_W_CACHE'
                _funding_io['brak_w_cache']+=1
            elif funding_aktualny(hist,now_ms):
                reason='CACHE_AKTUALNY'
                _funding_io['cache_poprawny']+=1
            else:
                reason='CACHE_WYMAGA_ODSWIEZENIA'
                _funding_io['cache_nieaktualny']+=1
            entries=len(hist) if isinstance(hist,list) else 0
            last=0
            if isinstance(hist,list):
                last=max((int(number(x.get('fundingTime')) or 0) for x in hist if isinstance(x,dict)),default=0)
            diag=funding_cache_diagnostyka_v138(hist,now_ms)
            funding_reasons.append({'symbol':sym,'powod':reason,'rekordy_w_cache':entries,
                                    'ostatni_funding_ms':last or None,'czas_zdjecia_ms':now_ms,
                                    'funding_z_pamieci':bool(coins[sym].get('funding_z_pamieci')),
                                    'blad':str((coins[sym].get('errors') or {}).get('funding_history','')),
                                    **{k:v for k,v in diag.items() if k != 'aktualny'}})
        # V6.139: oceniaj odpowiedz API po pobraniu, nie tylko stan cache przed nim.
        progress_prior = load_funding_progress_v139(drive_ok)
        progress_next = dict(progress_prior)
        progress_rows = []
        for sym in sorted(coins):
            old = fund_cache.get(sym)
            data = coins[sym]
            fetched = not bool(data.get('funding_z_pamieci'))
            prior = progress_prior.get(sym, {})
            if not isinstance(prior, dict):
                prior = {}
            prev_streak = int(prior.get('streak', 0) or 0)
            error = str((data.get('errors') or {}).get('funding_history') or '')
            diag_after = funding_progress_v139(old, data.get('funding_history'), fetched,
                                               error, prev_streak)
            if fetched:
                progress_next[sym] = {'streak': diag_after['kolejne_odswiezenia_bez_postepu'],
                                      'last_status': diag_after['status_postepu'],
                                      'last_run_utc': utc_now(),
                                      'last_funding_ms': diag_after['nowe_ostatnie_ms']}
            progress_rows.append({'symbol': sym, 'czy_pobrano_api': fetched, 'blad_api': error,
                                  **diag_after,
                                  **funding_opoznienie_publikacji_v140(old, now_ms,
                                      diag_after['status_postepu'],
                                      diag_after['kolejne_odswiezenia_bez_postepu'])})
        full_series_rows=[]
        for r in progress_rows:
            sym=r['symbol']
            full_series_rows.append({'symbol':sym,'czy_pobrano_api':r['czy_pobrano_api'],
                                     **funding_payload_audit_v146(fund_cache.get(sym),
                                        coins[sym].get('funding_history'),r['czy_pobrano_api'],r['blad_api'])})
        pd.DataFrame(full_series_rows).to_csv(OUT/'funding_pelna_seria_porownanie_v146.csv',index=False)
        full_by_symbol={r['symbol']:r for r in full_series_rows}
        progress_by_symbol={r['symbol']:r for r in progress_rows}
        audit147=[funding_diagnostyka_starej_serii_v147(sym,coins[sym],full_by_symbol[sym],progress_by_symbol[sym],now_ms) for sym in sorted(coins)]
        pd.DataFrame(audit147).to_csv(OUT/'funding_status_i_brak_postepu_v147.csv',index=False)
        audit148=[{**r,**funding_status_dostepnosc_v148(coins[r['symbol']],r)} for r in audit147]
        pd.DataFrame(audit148).to_csv(OUT/'funding_status_i_brak_postepu_v148.csv',index=False)
        _funding_io['diagnostyka_v147']=dict(pd.Series([r['status_diagnostyczny'] for r in audit147]).value_counts())
        _funding_io['pelna_seria_v146']=dict(pd.Series([r['status_pelnej_serii'] for r in full_series_rows]).value_counts())
        # V6.141 - chronologia rzeczywistych stawek (report-only).
        chronologia_rows=[]
        for r in progress_rows:
            sym=r['symbol']
            row=funding_chronologia_v141(fund_cache.get(sym),coins[sym].get('funding_history'),now_ms,
                                          r['czy_pobrano_api'],r['status_postepu'])
            row['symbol']=sym
            chronologia_rows.append(row)
        pd.DataFrame(chronologia_rows).to_csv(OUT/'funding_chronologia_v141.csv',index=False)
        distribution_rows=[funding_odstepy_v142(coins[sym].get('funding_history'),sym) for sym in sorted(coins)]
        pd.DataFrame(distribution_rows).to_csv(OUT/'funding_odstepy_historia_v142.csv',index=False)
        recent_rows=[r for sym in sorted(coins) for r in funding_ostatnie_cykle_v143(coins[sym].get('funding_history'),sym)]
        pd.DataFrame(recent_rows).to_csv(OUT/'funding_ostatnie_cykle_v143.csv',index=False)
        detail_rows=[r for sym in sorted(coins) for r in funding_odstepy_szczegoly_v144(coins[sym].get('funding_history'),sym)]
        pd.DataFrame(detail_rows,columns=['symbol','poczatek_utc','koniec_utc','poczatek_ms','koniec_ms','odstep_h','klasyfikacja','potwierdzona_zmiana_harmonogramu','uwaga']).to_csv(OUT/'funding_odstepy_szczegoly_v144.csv',index=False)
        rezim_rows=[{'symbol':sym,**funding_rezim_ocena_v144(funding_odstepy_szczegoly_v144(coins[sym].get('funding_history'),sym))} for sym in sorted(coins)]
        pd.DataFrame(rezim_rows).to_csv(OUT/'funding_rezim_ocena_v144.csv',index=False)
        regime145=[funding_rezim_v145(coins[sym].get('funding_history'),sym) for sym in sorted(coins)]
        pd.DataFrame(regime145).to_csv(OUT/'funding_rezim_porownanie_v145.csv',index=False)
        _funding_io['rezim_v145']=dict(pd.Series([r['status'] for r in regime145]).value_counts())
        _funding_io['szczegoly_v144']={'wiersze':len(detail_rows),'niejednoznaczne':sum('NIEJEDNOZNACZNY' in r['klasyfikacja'] for r in detail_rows)}
        _funding_io['ostatnie_cykle_v143']={'wiersze':len(recent_rows),'mozliwe_zmiany':sum(r['mozliwa_zmiana_cyklu'] for r in recent_rows)}
        _funding_io['rozkład_odstepow_v142']={'kontrakty':len(distribution_rows),'z_historia':sum(r['odstepow']>0 for r in distribution_rows)}
        _funding_io['chronologia_v141']=dict(pd.Series([r['status_chronologii'] for r in chronologia_rows]).value_counts())
        pd.DataFrame(progress_rows).to_csv(OUT/'funding_postep_odswiezen_v139.csv', index=False)
        pd.DataFrame(progress_rows).to_csv(OUT/'funding_opoznienia_publikacji_v140.csv', index=False)
        _funding_io['opoznienia_v140'] = dict(pd.Series([r['klasa_opoznienia'] for r in progress_rows]).value_counts())
        _funding_io['postep_v139'] = dict(pd.Series([r['status_postepu'] for r in progress_rows]).value_counts())
        _funding_io['postep_zapis_v139'] = save_funding_progress_v139(progress_next, drive_ok)
        warnings_139 = [r for r in progress_rows if r['ostrzezenie']]
        if warnings_139:
            print('  UWAGA: funding API bez postepu: ' + ', '.join(
                  f"{r['symbol']} ({r['status_postepu']}, kolejne={r['kolejne_odswiezenia_bez_postepu']})"
                  for r in warnings_139[:15]))
        # Dane API bez postepu nie sa uznawane za nowsze ani uzywane do
        # przedluzania waznosci cache. Dotychczasowa logika odswiezania zostaje.
        pd.DataFrame(funding_reasons).to_csv(OUT/'funding_cache_przyczyny.csv',index=False)
        pd.DataFrame(funding_reasons).to_csv(OUT/'funding_cache_cykl_v138.csv',index=False)
        _funding_io['cykl_v138'] = dict(pd.Series([x['powod_szczegolowy'] for x in funding_reasons]).value_counts())
        n_fz = sum(1 for d in coins.values() if d.get('funding_z_pamieci'))
        print('  API endpointy (liczba, czas HTTP): ' + ', '.join(
            f"{k.rsplit('/', 1)[-1]}={v['count']} / {v['seconds']:.1f}s"
            for k, v in sorted(_api_by_endpoint.items(), key=lambda kv: -kv[1]['seconds'])[:10]))
        n_api_pobieranie = _api['n']
        print(f"Zapytania API: {_api['n']} | funding z pamieci: {n_fz}/{len(coins)} | swiece bez pobierania: "
              f"{sum(1 for d in coins.values() for v in (d.get('klines_przyrost') or {}).values() if v == 0)}")
        save_funding_cache(coins, fund_cache, drive_ok)
        save_short_history(drive_ok)
        print('  historia swiec: ' + ', '.join(f'{k}={v}' for k, v in sorted(_fetch_stats.items()) if 'historia' in k or 'naprawa' in k))
        print('  tryby pobierania: ' + ', '.join(f'{k}={v}' for k, v in sorted(_fetch_stats.items())))
        print(f"  funding cache zapis: {_funding_io.get('zapis')}")
    run_utc = utc_now()
    print(f'Dane: {len(coins)} kontraktow, czas serwera {iso(now_ms)}')

    with etap('przygotowanie swiec'):
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
    _REJ['rows'] = []
    t_analiza = time.perf_counter()
    for sym, P in PP.items():
        d = P['raw']
        A = view(P)
        issues = dict(d.get('errors') or {})
        luki = gaps_qc(P)
        if luki:
            issues['luki_swiec'] = luki
        if P.get('zle_swiece'):
            issues['zle_swiece_usuniete'] = P['zle_swiece']      # V6.15
        if not enough(A):
            issues['za_malo_swiec'] = {tf: len(A.get(tf, [])) for tf in MIN_BARS}
            issues['historia_status'] = {
                tf: ('NATURALNIE_KROTKA_POTWIERDZONA' if (sym, tf) in _short_history_verified
                     else 'NIEKOMPLETNA_LUB_NIEPOTWIERDZONA')
                for tf in MIN_BARS if len(A.get(tf, [])) < MIN_BARS[tf]}
            # Brak MIN_BARS nadal uniemozliwia wygenerowanie setupu.
            qc[sym] = issues
            continue
        tick = d.get('ticker') or {}
        price = live_price(P) or float(A['5m'][-1, C])
        form = P['forming'].get('5m')
        ref = float(form[C]) if form is not None else float(A['5m'][-1, C])
        mism = abs(price / ref - 1) * 100 if ref else 0
        if mism > 0.5:
            issues['price_mismatch_pct'] = round(mism, 3)
        _REJ.update(on=True, sym=sym)                   # V6.15: odrzucone przy budowie -> odrzucone.csv
        try:
            f, setups = analyze(A, price, now_ms, P['fees'], btc_A if sym != 'BTCUSDT' else None,
                                crowd={'funding_z': funding_z_at(d, now_ms)})
        finally:
            _REJ['on'] = False
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
        if (P['asset_type'] in STOCK_TYPES and not us_open and not TRADFI_24H) or (     # V6.88: TRADFI_24H
                P['asset_type'] in TRADFI_TYPES and swiece_martwe(A['5m'])):
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
        w5 = A['5m'][A['5m'][:, T] >= now_ms - DANE_LUKI_H * HOUR_MS]
        luki5 = int((np.diff(w5[:, T]) > INTERVAL_MS['5m'] * 1.5).sum()) if len(w5) > 1 else 0
        if luki5 and P['asset_type'] not in TRADFI_TYPES:  # V6.15: TradFi ma przerwy sesji - tylko krypto
            issues['luki_5m_ostatnie'] = {'godziny': DANE_LUKI_H, 'luki': luki5}
            reasons.append('luki w danych')
        if wiek > DANE_MAX_WIEK_MIN or 'klines_5m' in bledy:
            issues['dane_nieaktualne'] = {'wiek_5m_min': round(float(wiek), 1), 'bledy': bledy}
            reasons.append('dane nieaktualne')
        tradable = not reasons
        if issues:
            qc[sym] = issues
        row = {'symbol': sym, 'live_price': price, 'tradable': tradable, 'not_tradable_reason': ', '.join(reasons),
               'asset_type': P['asset_type'], 'quote_vol_24h': qv, 'oi_usdt': rnd(oi, 0) if oi_ok else None,
               'oi_wiarygodne': oi_ok, 'oi_surowe_usdt': None if oi_ok else rnd(oi, 0), **{k: rnd(v, 3) for k, v in oich.items()},
               **oi_raw_fields(d),
               **{k: rnd(v, 6 if k == 'funding_rate' else 2) for k, v in fund.items()},
               **{k: rnd(v, 2) for k, v in book.items()},
               'taker_fee_bps': rnd(P['fees'][0], 2), 'maker_fee_bps': rnd(P['fees'][1], 2)}
        for k in FEATURE_COLS + ['struct_1h', 'last_event_1h', 'struct_15m', 'last_event_15m', 'vol_anomaly']:
            v = f.get(k)
            row[k] = rnd(v, 3) if isinstance(v, (float, np.floating)) else v
        for k in ELLIOTT_COLS:
            row[k] = f.get(k)
        # V6.157: ATR z oryginalnych swiec 1h, bez odtwarzania z zaokraglonych proporcji.
        row['elliott_atr_1h_v157'] = rnd(f.get('atr1'), 10)
        row['elliott_abc_c_utc_ms'] = f.get('elliott_abc_c_utc_ms')
        row['elliott_abc_wiek_h'] = f.get('elliott_abc_wiek_h')
        row['elliott_abc_od_c_atr'] = f.get('elliott_abc_od_c_atr')
        row['elliott_abc_swiezosc'] = f.get('elliott_abc_swiezosc')
        row['setups'] = len(setups)
        summary.append(row)
        for r in setups:
            if P['asset_type'] in STOCK_TYPES:
                r['filtr'] = ';'.join(x for x in (r['filtr'], 'tradfi_bez_testu') if x)
            ceny_do_ticka(r, P.get('tick_dec'))          # V6.15: ceny gotowe do wpisania na WEEX
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
    oi_rel = {}
    for r in summary:                                   # V6.16: wiarygodnosc OI (informacja, nie zmienia oceny)
        oi_reliability(r)
        oi_rel[r['symbol']] = (r['oi_wiarygodnosc'], r['oi_vs_gate'])
    for r in live:
        r['oi_wiarygodnosc'], r['oi_vs_gate'] = oi_rel.get(r['symbol'], ('bez potwierdzenia', None))
    oi_zmiana = {}                                      # V6.62/V6.68: prog OI po porownaniu z Gate
    for r in summary:
        pow_ = [x for x in (r.get('not_tradable_reason') or '').split(', ') if x]
        r['oi_prog_z_gate'] = False
        r['oi_zrodlo'] = 'WEEX' if r.get('oi_wiarygodne') else 'brak (OI WEEX > wolumen x limit - prog OI pominiety)'   # V6.63
        r['oi_override_powod'] = None
        if not any(x.startswith('OI <') for x in pow_):
            continue
        min_oi = MIN_OI_TRADFI_USDT if r.get('asset_type') in TRADFI_TYPES else MIN_OI_USDT
        g_oi = _num(r.get('gate_oi_usd'))               # None przy braku / NaN
        w_oi = _num(r.get('oi_usdt')) or _num(r.get('oi_surowe_usdt')) or 0
        if g_oi is not None and g_oi < min_oi:          # V6.68: niskie OI potwierdzone przez Gate -> blokada zostaje
            r['oi_zrodlo'] = 'WEEX + Gate (oba ponizej progu)'
            r['oi_override_powod'] = (f"OI WEEX {w_oi / 1e6:.2f}M i Gate {g_oi / 1e6:.2f}M (Gate {r.get('gate_wiek_h')} h) "
                                      f"< prog {min_oi / 1e6:.1f}M - blokada OI potwierdzona")
            continue
        if g_oi is None and not OI_PROG_TYLKO_Z_GATE:   # tryb sprzed V6.68: OI WEEX bez potwierdzenia blokuje
            continue
        pow_ = [x for x in pow_ if not x.startswith('OI <')]
        r['not_tradable_reason'], r['tradable'], r['oi_prog_z_gate'] = ', '.join(pow_), not pow_, True
        dalej = f"dalej blokuje: {', '.join(pow_)}" if pow_ else 'pozostale progi WEEX spelnione'
        if g_oi is not None:
            r['oi_zrodlo'] = 'Gate'
            r['oi_override_powod'] = (f"OI WEEX {w_oi / 1e6:.2f}M = {r.get('oi_vs_gate')}x Gate {g_oi / 1e6:.2f}M "
                                      f"(Gate {r.get('gate_wiek_h')} h) >= prog {min_oi / 1e6:.1f}M; {dalej}")
        else:
            r['oi_zrodlo'] = 'brak potwierdzenia (brak Gate) - prog OI pominiety'
            r['oi_override_powod'] = (f"OI WEEX {w_oi / 1e6:.2f}M < prog {min_oi / 1e6:.1f}M, ale bez potwierdzenia z Gate "
                                      f"(OI WEEX bywa bledne) - prog OI pominiety; {dalej}")
        oi_zmiana[r['symbol']] = (r['tradable'], r['not_tradable_reason'])
    oi_opis = {x['symbol']: (x.get('oi_zrodlo'), x.get('oi_override_powod')) for x in summary}   # V6.69
    for r in live:
        if r['symbol'] in oi_zmiana:
            r['tradable'], r['not_tradable_reason'] = oi_zmiana[r['symbol']]
        r['oi_zrodlo'], r['oi_override_powod'] = oi_opis.get(r['symbol'], (None, None))
        r['oi_niepotwierdzone'] = str(r.get('oi_zrodlo') or '').startswith('brak potwierdzenia')
    if oi_zmiana:
        opis = ', '.join(k + ' (' + (v[1] or 'do handlu') + ')' for k, v in oi_zmiana.items())
        print(f'Prog OI zdjety (Gate >= progu albo brak potwierdzenia z Gate): {opis}')
    try:
        log_cols = ['symbol', 'live_price', 'funding_rate', 'funding_z', 'basis_bps', 'oi_usdt', 'oi_chg_1h_pct', 'oi_chg_4h_pct',
                    'taker_buy_1h_pct', 'chg_1h_pct', 'chg_24h_pct', 'book_imbalance_0.5pct']
        log = pd.DataFrame([{k: r.get(k) for k in log_cols} for r in summary])
        log.insert(0, 'snapshot_utc', snap_iso(now_ms))
        log.to_csv(OUT / 'crowd_snapshot.csv', index=False)
        if drive_ok and not replay:
            dopisz_log(Path(DRIVE_DIR) / CROWD_LOG, log)          # V6.63
    except Exception as exc:
        print(f'Log fundingu/OI: blad ({str(exc)[:120]})')

    _czasy['analiza live + cechy + Gate'] = time.perf_counter() - t_analiza
    print(f"Analiza live + cechy + Gate: {_czasy['analiza live + cechy + Gate']:.1f}s")
    bt = []
    if BACKTEST:
        print('Test wsteczny ...')
        with etap('test wsteczny 7 dni'):
            bt_cache_wczytaj(drive_ok and not replay)                   # V6.98
            bt = run_backtest(PP, now_ms, oi_arr=oi_arrays(oi_hist), gidx=gidx, pamiec=True)
            print(f"  pamiec testu: {_bt_stat['trafienia']} z pamieci, {_bt_stat['liczone']} liczone od nowa")
            bt_cache_zapisz(drive_ok and not replay)
    bt = add_event_ids(pd.DataFrame(bt)).to_dict('records') if bt else bt   # V6.16
    # V6.153: tylko zdarzenia obecnego 7d testu; starsze archiwum bez pelnego OHLCV nie jest 'replayem'.
    replay153 = koszt_replay_ab_v153(bt, PP)
    replay153.to_csv(OUT / 'koszty_replay_ab_5m_v153.csv', index=False)
    if not replay153.empty and 'pokrycie' in replay153:
        rr = replay153[replay153['rozliczone_w_obu'].fillna(False).astype(bool)] if 'rozliczone_w_obu' in replay153 else pd.DataFrame()
        grp_cols = [c for c in ('typ_wersja','side','entry_type') if c in rr.columns]
        if len(rr) and grp_cols:
            agg = rr.groupby(grp_cols,dropna=False).agg(rozliczone=('delta_R_replay','size'),srednia_delta_R=('delta_R_replay','mean'),
                srednia_R_model=('r_model_replay','mean'),srednia_R_30=('r_30_replay','mean'),
                zmieniony_status=('status_rozny','sum')).reset_index()
        else: agg = pd.DataFrame(columns=grp_cols+['rozliczone','srednia_delta_R','srednia_R_model','srednia_R_30','zmieniony_status'])
    else: agg = pd.DataFrame()
    agg.to_csv(OUT / 'koszty_replay_ab_grupy_v153.csv', index=False)
    save_json('koszty_replay_ab_metoda_v153.json',{'version':VERSION,'zrodlo':'te_same_snapshoty_7d_i_swiece_5m',
       'tryb':'ponowna_symulacja_simulate_dwa_modele','bramka_produkcyjna':'BEZ_ZMIAN',
       'uwagi':['brak historycznego orderbooka','pozycje nierozliczone poza srednia',
                'brak pokrycia OHLCV nie jest zastępowany aproksymacja',
                'brak pelnego drugiego gate i benchmarku; nie wolno wywodzic zmiany decyzji handlowej']},pretty=True)
    bt_df = write_csv('backtest.csv', bt, BT_FIRST)
    arch = update_archive(bt, run_utc, drive_ok, now_ms)
    arch.to_csv(OUT / 'backtest_archive.csv', index=False)
    # V6.155: pelne kryteria gate_table liczone ponownie, ale TYLKO na sparowanym zbiorze 7d.
    # Nie nalezy mylic tego z produkcyjna bramka bazujaca na calym archiwum.
    gate155, meta155 = koszt_gate_replay_v155(bt, replay153, now_ms, od=reguly_od(arch, trwale=False))
    gate155.to_csv(OUT / 'koszty_gate_ab_7d_v155.csv', index=False)
    save_json('koszty_gate_ab_metoda_v155.json', meta155, pretty=True)
    # V6.151: TEN SAM zbior historycznych zdarzen; bez przeliczania regul bramki.
    cost_det151,cost_summary151=cost_comparison_v151(arch)
    cost_det151.to_csv(OUT / 'koszty_5_10_12_15_vs_30_szczegoly_v151.csv',index=False)
    cost_legs_v152(cost_det151).to_csv(OUT / 'koszty_nogi_v152.csv',index=False)
    gate_cost_diagnostic_v152(arch,cost_det151,None).to_csv(OUT / 'koszty_bramka_prog_badawczy_v152.csv',index=False)
    save_json('koszty_metodologia_v152.json',{'version':VERSION,'charakter':'aproksymacja na identycznych rozliczeniach', 'nie_przelicza':['fill','exit','1R czesciowe','gate_table','t_edge','waga snapshot','koszt prowizji'],'bramka_bez_zmian':True,'model_porownawczy_bps':30},pretty=True)
    cost_summary151.to_csv(OUT / 'koszty_5_10_12_15_vs_30_grupy_bramki_v151.csv',index=False)
    save_json('koszty_model_porownanie_v151.json', {
        'version':VERSION,'model_aktualny':HISTORICAL_SLIP_MODEL,
        'model_porownawczy':'STALE_30_BPS','rekordy_ocenialne':int(len(cost_det151)),
        'metoda':'aproksymacja roznicy kosztu na tych samych wykonanych zdarzeniach',
        'ograniczenia':['brak ponownego replay OHLCV','nie zmienia fill/exit',
                        'entry MARKET moze roznic sie od rzeczywistego fill',
                        'nie aktualizuje gate.csv ani statusow handlowych',
                        'nie liczy dokladnie kosztu czesciowych wyjsc 1R',
                        'wyniki nie sa dowodem przewagi OOS']},pretty=True)

    arch_tf, arch = split_stocks(arch)               # V6.14: akcje/indeksy osobno - logika krypto bez zmian
    arch, _schemat_krypto_v170 = schemat_bez_duplikatow_v170(arch, 'arch_krypto')
    arch_tf, _schemat_tradfi_v170 = schemat_bez_duplikatow_v170(arch_tf, 'arch_tradfi')
    _schemat_log_v170 = _schemat_krypto_v170 + _schemat_tradfi_v170
    write_csv('kolumny_wewnetrzne_audyt_v174.csv', _duplikaty_rekordow_v174, ('raport','kolumna','liczba_wystapien','usuniete_kopie','status'))
    write_csv('kolumny_duplikaty_audyt_v170.csv', _schemat_log_v170,
              ('raport','kolumna','liczba_wystapien','usuniete_kopie','status'))
    if _schemat_log_v170:
        print('V6.170: scalono identyczne kopie kolumn raportowych:', len(_schemat_log_v170))

    if FAST_SCAN:
        print('FAST_SCAN: pomijam kosztowne raporty badawcze Elliott/THREE_WAY/MARKET_AB (bez zmiany bramki).')
    else:
        with etap('raporty badawcze portfela'):
            # V6.76: raport diagnostyczny, nie wpływa na bramkę ani decyzje handlowe.
            # (poprawka: po split_stocks - tylko krypto, jak bramka; i tylko setupy po filtrach - jak bramka i portfel)
            try:
                ell_src = elliott_tylko_po_filtrach(arch)
                elliott_statystyki(ell_src).to_csv(OUT / 'elliott_stats.csv', index=False)
                ell_ab, ell_week, ell_exit = elliott_portfel_ab(ell_src, return_weekly=True, return_exit_weekly=True)
                ell_ab.to_csv(OUT / 'elliott_portfolio_ab.csv', index=False)
                ell_week.to_csv(OUT / 'elliott_portfolio_ab_weekly.csv', index=False)
                ell_exit.to_csv(OUT / 'elliott_portfolio_ab_exit_weekly.csv', index=False)
                daily_ab=elliott_ab_dzienny(ell_src)
                daily_ab.to_csv(OUT / 'elliott_portfolio_ab_daily.csv',index=False)
                elliott_ab_odpornosc(daily_ab).to_csv(OUT / 'elliott_ab_robustness.csv',index=False)
                loo,stable=elliott_ab_leave_one_day_out(daily_ab)
                loo.to_csv(OUT / 'elliott_ab_leave_one_day_out.csv',index=False)
                stable.to_csv(OUT / 'elliott_ab_stability.csv',index=False)
                elliott_ab_werdykt(daily_ab).to_csv(OUT / 'elliott_ab_verdict.csv',index=False)
                stress_rows=[]
                for slots in (2,3):
                    for cost in (0.0,0.05,0.10):
                        for reverse in (False,True):
                            st=elliott_portfel_ab(ell_src,miejsca=slots,extra_cost_R=cost,tie_reverse=reverse)
                            for record in st.to_dict('records'):
                                stress_rows.append({'miejsca_kierunek':slots,'dodatkowy_koszt_R':cost,
                                                    'kolejnosc_remisow':'odwrotna' if reverse else 'normalna',**record})
                stress_ab=pd.DataFrame(stress_rows)
                stress_ab.to_csv(OUT / 'elliott_portfolio_ab_stress.csv',index=False)
                elliott_ab_ocena_kosztow(stress_ab).to_csv(OUT / 'elliott_ab_cost_verdict.csv',index=False)
                # OOS: wczesniejsze dane sluza projektowaniu. Data graniczna jest stala.
                future=ell_src.copy()
                if 'snapshot_utc' in future.columns:
                    future=future[pd.to_datetime(future['snapshot_utc'],utc=True,errors='coerce')>=pd.Timestamp('2026-10-27T00:00:00Z')]
                elliott_statystyki(future).to_csv(OUT / 'elliott_stats_oos.csv',index=False)
                elliott_oos_jakosc(future).to_csv(OUT / 'elliott_oos_quality.csv',index=False)
                ell_oos_engine=elliott_portfel_ab(future,audit_trace=True)
                ell_oos_engine.to_csv(OUT / 'elliott_portfolio_ab_oos.csv',index=False)
                # V6.125: raport faktycznie wybranych przez silnik transakcji Elliott OOS.
                ell_tx,ell_days,ell_conc=audyt_portfela_v125(
                    elliott_portfel_ab._last_trade_events,None,'ELLIOTT_OOS')
                # V6.134: persist separate hashes in same historical checkpoint.
                v134_hashes=audit134_checkpoints(ell_tx,None)
                if len(v134_hashes):
                    cols=['wariant','event_id','symbol','snapshot_utc','input_sha256','output_sha256']
                    ell_tx=ell_tx.merge(v134_hashes[cols],on=cols[:4],how='left',validate='one_to_one')
                else:
                    ell_tx['input_sha256']=pd.Series(dtype=str)
                    ell_tx['output_sha256']=pd.Series(dtype=str)
                ell_tx.to_csv(OUT / 'audyt_elliott_oos_transakcje.csv',index=False)
                sensitivity_oos_v137(ell_tx).to_csv(OUT / 'elliott_oos_slippage_scenariusze_v137.csv',index=False)
                save_json('elliott_oos_slippage_model_v137.json', {'version':VERSION,'fallback_slip_bps':float(HISTORICAL_SLIP_BPS),'model':HISTORICAL_SLIP_MODEL,'scenariusze_bps':[10,20,30], 'rodzaj':'badawcza aproksymacja liniowa, bez nowej symulacji','bez_lookahead_book':True},pretty=True)
                ell_days.to_csv(OUT / 'audyt_elliott_oos_dni.csv',index=False)
                ell_conc.to_csv(OUT / 'audyt_elliott_oos_koncentracja.csv',index=False)
                # V6.127: kontrola sum bez tworzenia nowej symulacji ani zmiany decyzji.
                ell_check=kontrola_sum_elliott_v127(ell_oos_engine,ell_tx)
                ell_check.to_csv(OUT / 'audyt_elliott_oos_zgodnosc.csv',index=False)
                if (ell_check['status']=='NIEZGODNE').any():
                    print('UWAGA: audyt Elliott OOS NIEZGODNY z silnikiem portfela!')
                ell_ck=Path(DRIVE_DIR) / 'audyt_elliott_oos_state_v127.csv'
                ell_prior=None
                if drive_ok and not replay and ell_ck.exists():
                    try:ell_prior=pd.read_csv(ell_ck,dtype=str)
                    except Exception as exc:print(f'Audyt Elliott: brak porownania checkpoint ({str(exc)[:120]})')
                # Nie utozsamiaj pustego pliku i bledu odczytu z prawidlowym checkpointem.
                ell_checkpoint_status=('BRAK_POPRZEDNIEGO_PUNKTU' if not ell_ck.exists() else
                                       'NIEODCZYTANY_LUB_USZKODZONY' if ell_prior is None else
                                       'ODCZYTANY')
                ell_delta=audyt_zmian_elliott_v127(ell_tx,ell_prior)
                ell_delta.to_csv(OUT / 'audyt_elliott_oos_zmiany.csv',index=False)
                ell_reason=audyt_przyczyn_elliott_v129(ell_delta,ell_tx,ell_prior)
                ell_reason.to_csv(OUT / 'audyt_elliott_oos_przyczyny_zmian.csv',index=False)
                ell_trace=audit130_diagnose(ell_tx,ell_prior)
                ell_trace.to_csv(OUT / 'audyt_elliott_oos_slady_zmian.csv',index=False)
                audit132_classify(ell_tx,ell_prior).to_csv(OUT / 'audyt_elliott_oos_klasyfikacja_v132.csv',index=False)
                audit134_details(ell_tx,ell_prior).to_csv(OUT / 'audyt_elliott_oos_pola_przed_po_v134.csv',index=False)
                audit135_slippage_provenance(ell_tx,ell_prior).to_csv(OUT / 'audyt_elliott_oos_slippage_v135.csv',index=False)
                audit134_checkpoints(ell_tx,ell_prior).to_csv(OUT / 'audyt_elliott_oos_hash_wejsc_wynikow_v134.csv',index=False)
                save_json('audyt_elliott_oos_slady_meta.json',{'wersja':VERSION,
                    'silnik_sha256':_audit130_engine(),
                    'liczba_rozliczen':len(ell_tx),
                    'brak_snapshotu_OHLCV':int((ell_tx['status_zrodla_OHLCV']=='BRAK_PELNEGO_SNAPSHOTU_OHLCV').sum()) if len(ell_tx) else 0,
                    'tolerancja_diagnostyczna_R':0.001,
                    'ostrzezenie':'Fingerprint rekordu nie jest fingerprintem swiec; bez OHLCV nie ustala zmian zrodlowych.'})
                if (ell_reason['typ_zmiany']=='ZMIANA_ROZLICZENIA').any():
                    print('Audyt Elliott: wykryto zmienione rozliczenia; przyczyna zrodlowa NIEUSTALONA bez dwoch snapshotow wejsciowych.')
                save_json('audyt_elliott_oos_checkpoint_status.json', {'status':ell_checkpoint_status, 'checkpoint':ell_ck.name, 'wiersze_poprzednio':len(ell_prior) if ell_prior is not None else None, 'wiersze_teraz':len(ell_tx), 'typy_zmian':ell_delta['typ_zmiany'].value_counts().to_dict() if not ell_delta.empty else {}, 'kontrola_sum':ell_check['status'].value_counts().to_dict()})
                if drive_ok and not replay and not _ARCH_BLAD and not (ell_check['status']=='NIEZGODNE').any():
                    try:zapisz_atomowo(ell_ck,lambda tmp:ell_tx.to_csv(tmp,index=False))
                    except Exception as exc:print(f'Audyt Elliott: blad zapisu checkpoint ({str(exc)[:120]})')
                oos_daily=elliott_ab_dzienny(future,poczatek='2026-10-27T00:00:00Z')
                oos_daily.to_csv(OUT / 'elliott_portfolio_ab_oos_daily.csv',index=False)
                elliott_ab_odpornosc(oos_daily).to_csv(OUT / 'elliott_ab_robustness_oos.csv',index=False)
                oos_loo,oos_stable=elliott_ab_leave_one_day_out(oos_daily)
                oos_loo.to_csv(OUT / 'elliott_ab_leave_one_day_out_oos.csv',index=False)
                oos_stable.to_csv(OUT / 'elliott_ab_stability_oos.csv',index=False)
                oos_progress=elliott_oos_postep(future,oos_daily)
                oos_progress.to_csv(OUT / 'elliott_oos_progress.csv',index=False)
                oos_verdict=elliott_ab_werdykt(oos_daily)
                oos_verdict.to_csv(OUT / 'elliott_ab_verdict_oos.csv',index=False)
                elliott_oos_niezaleznosc(future).to_csv(OUT / 'elliott_oos_independence.csv',index=False)
                oos_phases=elliott_oos_fazy(future)
                oos_phases.to_csv(OUT / 'elliott_oos_phase_results.csv',index=False)
                elliott_oos_alarm(oos_progress,oos_phases,oos_verdict).to_csv(OUT / 'elliott_oos_alert.csv',index=False)
                # V6.86: opcjonalny stan diagnostyczny, niezalezny od archiwum handlowego.
                checkpoint=Path(DRIVE_DIR) / OOS_CHECKPOINT_V686
                prior=None
                if drive_ok and not replay and checkpoint.exists():
                    try:
                        prior=json.loads(checkpoint.read_text(encoding='utf-8'))
                    except (OSError,ValueError,TypeError) as exc:
                        print(f'OOS V6.86: nieczytelny stan porownania ({str(exc)[:100]})')
                delta, new_state=elliott_oos_delta(future,prior)
                delta.to_csv(OUT / 'elliott_oos_delta.csv',index=False)
                elliott_oos_populacje(oos_progress,oos_phases,elliott_portfel_ab(future)).to_csv(
                    OUT / 'elliott_oos_population.csv',index=False)
                if drive_ok and not replay and not _ARCH_BLAD:
                    try:
                        zapisz_atomowo(checkpoint,lambda tmp: Path(tmp).write_text(
                            json.dumps(new_state,ensure_ascii=False,separators=(',',':')),encoding='utf-8'))
                    except Exception as exc:
                        print(f'OOS V6.86: brak zapisu porownania ({str(exc)[:100]}); handel bez zmian')
                elliott_oos_dzienna_jakosc(oos_daily).to_csv(OUT / 'elliott_oos_daily_quality.csv',index=False)
            except Exception as exc:
                print(f'Raport Elliotta: blad ({str(exc)[:150]})')
                pd.DataFrame(columns=['grupa','transakcje','sredni_R_netto']).to_csv(OUT / 'elliott_stats.csv',index=False)
                pd.DataFrame(columns=['wariant','transakcje','suma_R']).to_csv(OUT / 'elliott_portfolio_ab.csv',index=False)
                pd.DataFrame(columns=['wariant','tydzien_utc','transakcje','suma_R']).to_csv(OUT / 'elliott_portfolio_ab_weekly.csv',index=False)
                for nm in ('elliott_portfolio_ab_exit_weekly.csv','elliott_portfolio_ab_stress.csv','elliott_stats_oos.csv','elliott_portfolio_ab_oos.csv','elliott_oos_quality.csv','elliott_portfolio_ab_daily.csv','elliott_portfolio_ab_oos_daily.csv','elliott_oos_daily_quality.csv','elliott_ab_robustness.csv','elliott_ab_robustness_oos.csv','elliott_ab_cost_verdict.csv','elliott_ab_leave_one_day_out.csv','elliott_ab_stability.csv','elliott_ab_leave_one_day_out_oos.csv','elliott_ab_stability_oos.csv','elliott_ab_verdict.csv','elliott_ab_verdict_oos.csv','elliott_oos_progress.csv','elliott_oos_independence.csv','elliott_oos_phase_results.csv','elliott_oos_alert.csv','elliott_oos_delta.csv','elliott_oos_population.csv'):
                    pd.DataFrame(columns=['wariant','dzien_utc','suma_R'] if 'daily' in nm else ['wariant','suma_R']).to_csv(OUT / nm,index=False)
            try:                                               # V6.122: niezalezne OOS wg wejscia i swiezych ABC
                oos_groups,oos_events=raport_trzech_wejsc_oos(arch)
                oos_groups.to_csv(OUT / 'wejscia_market_limit_elliott_oos.csv',index=False)
                oos_events.to_csv(OUT / 'wejscia_market_limit_elliott_zdarzenia.csv',index=False)
                # V6.123: sprawdzony tylko wspolny snapshot/symbol/strategia/strona.
                pm,pe,pdzien=raport_parowany_wejsc_oos(arch)
                pm.to_csv(OUT / 'wejscia_market_limit_parowane_oos.csv',index=False)
                pe.to_csv(OUT / 'wejscia_market_limit_pary.csv',index=False)
                pdzien.to_csv(OUT / 'wejscia_market_limit_pary_dziennie.csv',index=False)
                # V6.124: wspolne decyzje, brak wypelnienia i chronologiczny portfel.
                dec,dece,decport=raport_decyzji_wejsc_oos(arch)
                dec.to_csv(OUT / 'wejscia_market_limit_decyzje_oos.csv',index=False)
                dece.to_csv(OUT / 'wejscia_market_limit_decyzje_szczegoly.csv',index=False)
                decport.to_csv(OUT / 'wejscia_market_limit_portfel_oos.csv',index=False)
                # V6.125: porownanie statusow par miedzy przebiegami, atomowy checkpoint.
                ck=Path(DRIVE_DIR) / 'pary_oos_state_v125.csv'
                prior_pairs=None
                if drive_ok and not replay and ck.exists():
                    try:prior_pairs=pd.read_csv(ck,dtype=str)
                    except Exception as exc:print(f'Pary OOS: blad odczytu stanu ({str(exc)[:100]})')
                changes=audyt_par_v125(dece,prior_pairs)
                if prior_pairs is None:print('Pary OOS: brak wiarygodnego poprzedniego checkpointu (pierwsze uruchomienie lub blad odczytu)')
                changes.to_csv(OUT / 'wejscia_market_limit_zmiany_par.csv',index=False)
                if drive_ok and not replay and not _ARCH_BLAD:
                    try:zapisz_atomowo(ck,lambda tmp: dece.to_csv(tmp,index=False))
                    except Exception as exc:print(f'Pary OOS: blad zapisu stanu ({str(exc)[:100]})')
                # Portfel: tylko wykonane transakcje rzeczywiscie wybrane przez silnik.
                # Podzial wg strategii/dnia uzywa tego samego algorytmu co porownanie OOS.
                port_events=[]
                for typ in ('MARKET','LIMIT'):
                    source=elliott_tylko_po_filtrach(arch)
                    source=source.loc[source['entry_type'].astype(str).str.upper().eq(typ)].copy()
                    event_col=('market_event_id' if typ=='MARKET' else 'limit_event_id')
                    valid_events=set(dece[event_col].dropna().astype(str)) if event_col in dece else set()
                    source=source.loc[source['event_id'].astype(str).isin(valid_events)]
                    source=source.loc[pd.to_datetime(source['snapshot_utc'],utc=True,errors='coerce') >= pd.Timestamp('2026-10-27T00:00:00Z')]
                    elliott_portfel_ab(source,miejsca=2,warianty=('BAZA',),asof_utc=pd.to_datetime(source['snapshot_utc'],utc=True,errors='coerce').max() if len(source) else None)
                    for r in elliott_portfel_ab._last_trade_events:
                        port_events.append({**r,'wariant':typ+'_SLOT2'})
                ptx,pdays,pstats=audyt_portfela_v125(port_events,None,'MARKET_LIMIT_OOS')
                ptx.to_csv(OUT / 'audyt_wejsc_portfel_transakcje.csv',index=False)
                pdays.to_csv(OUT / 'audyt_wejsc_portfel_dni.csv',index=False)
                pstats.to_csv(OUT / 'audyt_wejsc_portfel_koncentracja.csv',index=False)
            except Exception as exc:
                print(f'Raport WEJSC OOS: blad ({str(exc)[:150]})')
                raport_trzech_wejsc_oos(pd.DataFrame())[0].to_csv(OUT / 'wejscia_market_limit_elliott_oos.csv',index=False)
                raport_trzech_wejsc_oos(pd.DataFrame())[1].to_csv(OUT / 'wejscia_market_limit_elliott_zdarzenia.csv',index=False)
                for fn,frame in zip(('wejscia_market_limit_parowane_oos.csv','wejscia_market_limit_pary.csv','wejscia_market_limit_pary_dziennie.csv'),raport_parowany_wejsc_oos(pd.DataFrame())):
                    frame.to_csv(OUT / fn,index=False)
                for fn,frame in zip(('wejscia_market_limit_decyzje_oos.csv','wejscia_market_limit_decyzje_szczegoly.csv','wejscia_market_limit_portfel_oos.csv'),raport_decyzji_wejsc_oos(pd.DataFrame())):
                    frame.to_csv(OUT / fn,index=False)
            try:                                               # V6.89: porownywalny benchmark 3 rankingow
                portfolio_three_way(elliott_tylko_po_filtrach(arch)).to_csv(
                    OUT / 'portfolio_three_way.csv', index=False)
            except Exception as exc:
                print(f'Raport THREE_WAY: blad ({str(exc)[:150]})')
                portfolio_three_way(pd.DataFrame()).to_csv(OUT / 'portfolio_three_way.csv', index=False)
            try:                                               # V6.90: stabilnosc 3 rankingow wg dni ZAMKNIECIA
                d, loo, st = portfolio_three_way_daily_stability(elliott_tylko_po_filtrach(arch))
                d.to_csv(OUT / 'portfolio_three_way_daily.csv', index=False)
                loo.to_csv(OUT / 'portfolio_three_way_leave_one_day_out.csv', index=False)
                st.to_csv(OUT / 'portfolio_three_way_stability.csv', index=False)
                portfolio_three_way_daily_stability._symbol_rows.to_csv(OUT / 'portfolio_three_way_symbol_stability.csv',index=False)
                portfolio_three_way_verdict(st).to_csv(OUT / 'portfolio_three_way_verdict.csv',index=False)
            except Exception as exc:
                print(f'Raport THREE_WAY STABILITY: blad ({str(exc)[:150]})')
                d, loo, st = portfolio_three_way_daily_stability(pd.DataFrame())
                d.to_csv(OUT / 'portfolio_three_way_daily.csv', index=False)
                loo.to_csv(OUT / 'portfolio_three_way_leave_one_day_out.csv', index=False)
                st.to_csv(OUT / 'portfolio_three_way_stability.csv', index=False)
                pd.DataFrame(columns=SYMBOL_STABILITY_COLS).to_csv(OUT / 'portfolio_three_way_symbol_stability.csv',index=False)
                portfolio_three_way_verdict(st).to_csv(OUT / 'portfolio_three_way_verdict.csv',index=False)
            try:                                               # V6.87: kolejnosc wyboru MARKET vs LIMIT (tylko raport)
                market_ab_raport(elliott_tylko_po_filtrach(arch)).to_csv(OUT / 'portfolio_market_ab.csv', index=False)
            except Exception as exc:
                print(f'Raport MARKET_PIERWSZE: blad ({str(exc)[:150]})')
                pd.DataFrame(columns=['populacja','okres','wariant','suma_R']).to_csv(OUT / 'portfolio_market_ab.csv', index=False)
    tf_tab = tradfi_table(arch_tf, now_ms)
    if len(tf_tab):
        tf_tab.to_csv(OUT / 'tradfi_test.csv', index=False)
    arch_od = arch
    if replay:                                          # replay: historia uruchomien z archiwum w zipie
        try:
            with zipfile.ZipFile(REPLAY_ZIP) as z:
                nm = next((n for n in z.namelist() if n.endswith('backtest_archive.csv')), None)
                if nm:
                    arch_od = pd.concat([pd.read_csv(io.BytesIO(z.read(nm)), low_memory=False), arch], ignore_index=True)
        except Exception:
            pass
    od = reguly_od(arch_od, trwale=drive_ok and not replay)   # V6.55                             # V6.17: od kiedy reguly danej wersji sa ustalone
    gate = gate_table(arch, now_ms, od)
    # Status statystyczny bramki nie oznacza zgody na handel typem wylaczonym.
    if len(gate):
        # V6.79 (poprawka): typ z '.M' to wejscie MARKET, bez '.M' - LIMIT. Wczesniej wiersz LIMIT byl
        # 'WYLACZONY' tylko gdy wylaczone byly OBA wejscia, wiec CONT_SWEEP.v4 LONG (LIMIT wylaczony
        # w WYLACZONE_WEJSCIA) pokazywal sie jako dozwolony.
        gate['typ_wylaczony_konfiguracja'] = [bool(typ_wylaczony(str(r.typ).split('.')[0], r.side,
                'MARKET' if str(r.typ).endswith('.M') else 'LIMIT')) for r in gate.itertuples()]
        gate['status_operacyjny_typu'] = np.where(gate['typ_wylaczony_konfiguracja'], 'WYLACZONY',
                                                   'PATRZ FILTRY WEJSCIA I DO_HANDLU')
    gate.to_csv(OUT / 'gate.csv', index=False)
    gmap = {(g.typ, g.side): g for g in gate.itertuples()}
    for r in live:
        g = gmap.get((r['typ_wersja'], r['side']))
        r['status_bramki'] = g.status if g is not None else 'OBSERWACJA'
        r['typ_transakcje'], r['typ_sredni_R'], r['typ_t'] = (g.transakcje, g.sredni_R, g.t) if g is not None else (0, None, None)
        r['po_reg_n'], r['po_reg_R'], r['po_reg_t'] = (g.transakcje, g.sredni_R, g.t_edge) if g is not None else (0, None, None)
        r['po_reg_edge'] = g.sredni_edge_R if g is not None else None
        r['po_reg_okazje'], r['po_reg_od'] = (g.unikalne_okazje, g.od) if g is not None else (0, None)
    try:                                                # V6.36: hipoteza KOMPRESJA (CONT_SWEEP SHORT)
        hip = hipoteza_table(arch)
        hip.to_csv(OUT / 'hipotezy.csv', index=False)
        apply_hipoteza(live, hip)
        h0 = hip.iloc[0]
        print(f"Hipoteza {h0['hipoteza']} (od {HIPOTEZA_OD} UTC): {h0['status']}, transakcje {h0['transakcje']}, "
              f"sr {h0['sredni_R']}R, t {h0['t']} (progi: >= {GATE_MIN_TRADES}, >= {GATE_MIN_AVG_R}R, t >= {GATE_MIN_T})")
    except Exception as exc:                           # V6.37: blad widoczny w pliku i w run_meta, nie tylko w logu
        print(f'Hipoteza: BLAD ({str(exc)[:150]})')
        _HIP_BLAD['blad'] = str(exc)[:200]
        try:
            pd.DataFrame([{'hipoteza': 'KOMPRESJA CONT_SWEEP SHORT', 'status': f'BLAD: {str(exc)[:150]}',
                           'od': HIPOTEZA_OD}]).to_csv(OUT / 'hipotezy.csv', index=False)
        except Exception:
            pass
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
    htab = hist_table(hist_df, od)
    var_df = hist_meta.pop('_warianty', None) if isinstance(hist_meta, dict) else None
    if var_df is not None and len(var_df):
        var_df.to_csv(OUT / 'hist_warianty.csv', index=False)
    if len(hist_df):
        hist_df = add_event_ids(hist_df)                    # V6.16
        hist_df.to_csv(OUT / 'hist_backtest.csv', index=False)
    jak = (jakosc_testu(bt_df, 'test 7 dni', od) + jakosc_testu(arch, f'archiwum {BT_ARCHIVE_DAYS}d', od)
           + jakosc_testu(hist_df, f'historia {HIST_DAYS}d', od))
    if jak:
        pd.DataFrame(jak).to_csv(OUT / 'jakosc_testu.csv', index=False)
    rozb = []                                           # V6.24: dodatnie w calej probie, ujemne po ustaleniu regul
    if len(htab) and 'po_reg_R' in htab.columns:
        htab['rozbieznosc'] = ''
        for i, h in htab.iterrows():
            a_, b_ = _num(h.get('sredni_R')), _num(h.get('po_reg_R'))
            if a_ is not None and b_ is not None and a_ > 0 and b_ < 0 and (h.get('po_reg_n') or 0) >= ROZB_MIN_N:
                htab.at[i, 'rozbieznosc'] = 'dodatnie w 30 dniach, ujemne po ustaleniu regul'
                rozb.append(f"{h['typ']} {h['side']}: 30 dni {a_:+.3f}R (n={h['transakcje']}) -> po ustaleniu regul {b_:+.3f}R (n={h['po_reg_n']})")
    if len(hist_df):
        htab.to_csv(OUT / 'hist_summary.csv', index=False)
    zakresy = {}                                        # V6.21: nakladanie sie testow w czasie
    for nm_, df_ in (('test 7 dni', bt_df), (f'archiwum {BT_ARCHIVE_DAYS}d', arch), (f'historia {HIST_DAYS}d', hist_df)):
        if df_ is not None and len(df_) and 'snapshot_utc' in df_.columns:
            zakresy[nm_] = (str(df_.snapshot_utc.min())[:16], str(df_.snapshot_utc.max())[:16])
    cechy = cechy_table(hist_df, f'historia {HIST_DAYS}d') + cechy_table(arch, f'archiwum WEEX {BT_ARCHIVE_DAYS}d')
    if cechy:
        pd.DataFrame(cechy).to_csv(OUT / 'hist_cechy.csv', index=False)
    order = {'DO GRY': 0, 'PROBA': 1, 'OBSERWACJA': 2}
    score_setups(live, arch, now_ms, hist_df)
    apply_tradfi(live, tf_tab)
    btc_bias = btc_short_bias(btc_A)
    rate_setups(live, btc_bias)
    ochrona = protection_mode(arch, now_ms)
    for r in live:
        raw_ = (PP.get(r['symbol']) or {}).get('raw') or {}
        r.update(ksiazka_setup(raw_.get('depth'), r, (raw_.get('ticker') or {}).get('quoteVolume')))   # V6.61
    for r in live:
        m, why_m = 1.0, []
        if ochrona_strony(ochrona, r['side']) and r.get('setup_type') not in OCHRONA_BEZ_TYPOW:   # V6.60
            m *= OCHRONA_MNOZNIK
            why_m.append(f'TRYB OCHRONNY: ryzyko x{OCHRONA_MNOZNIK}')
        if BRAMKA_TRYB == 'probny' and r.get('status_bramki') != 'DO GRY':   # V6.19
            m *= PROBA_MNOZNIK
            why_m.append(f'typ bez zatwierdzenia (PROBA): ryzyko x{PROBA_MNOZNIK}')
        r['ryzyko_mnoznik'] = m
        if m != 1.0:
            try:
                r.update(leverage_plan(r, PP[r['symbol']], m))
                r['dzwignia_uwagi'] = '; '.join(x for x in [r.get('dzwignia_uwagi')] + why_m if x)
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
    # V6.43: FAIL CLOSED. Brak trwalego dziennika = brak pewnosci liczby aktywnych PROBA,
    # limitu strat i wyniku po ustaleniu regul. Replay jest tylko analiza historyczna.
    if not drive_ok and not replay:
        for r in live:
            r['do_handlu'] = False
            w = str(r.get('warunki') or '')
            r['warunki'] = (w + '; ' if w and w != 'OK' else '') + 'BRAK DYSKU: brak trwalej walidacji PROBA/bramki'
        warunki_glob.append('brak Google Drive - handel zablokowany')
    if _ARCH_BLAD and not replay:                       # V6.59: archiwum nieczytelne = brak historii bramki/ochrony
        for r in live:
            r['do_handlu'] = False
            w = str(r.get('warunki') or '')
            r['warunki'] = (w + '; ' if w and w != 'OK' else '') + 'ARCHIWUM: blad odczytu - brak historii bramki'
        warunki_glob.append('archiwum: blad odczytu - handel zablokowany')
    proba_meta = proba_select_and_log(live, PP, drive_ok, now_ms)   # V6.43: tylko z trwalym dziennikiem
    niezgodne = []
    for r in live:
        r['status_setupu'] = status_setupu(r)          # V6.22
        r['pewnosc_przewagi'] = pewnosc_przewagi(r)    # V6.23
        w_ = [x.strip() for x in str(r.get('warunki') or '').split(';') if x.strip() and x.strip() != 'OK']
        bez = ('tryb ochronny', 'alarm BTC', 'dzienny limit', 'nie tradable')   # V6.30
        r['blokady_bezpieczenstwa'] = '; '.join(x for x in w_ if x.startswith(bez))
        r['blokady_kwalifikacji'] = '; '.join(x for x in w_ if not x.startswith(bez))
        if (r['status_setupu'] in ('DO GRY', 'PROBA', 'BEZ BRAMKI')) != bool(r.get('do_handlu')):
            niezgodne.append(f"{r['symbol']} {r['side']} {r.get('setup_type')}")
    if niezgodne:
        print(f"!!! NIEZGODNOSC status_setupu / do_handlu: {', '.join(niezgodne[:10])}")
    for r in live:
        r['w_limicie_ryzyka'] = False                  # V6.9: ustawiane na True w wybor_ryzyka
    rank_rows = ranking(live)
    ekspozycja = {sd: {'setupy_AB': len({r['symbol'] for r in rank_rows if r['side'] == sd and r['ocena'] in ('A', 'B')}),
                       'pozycje': sum(1 for p in pos_rows if p.get('side') == sd),
                       'pozycje_z_ryzykiem': pozycje_z_ryzykiem(positions, sd)} for sd in ('LONG', 'SHORT')}
    for sd, e in ekspozycja.items():                   # V6.5: MARKET i LIMIT na tym samym kontrakcie = jedna transakcja
        ryz = RYZYKO_NA_TRANSAKCJE_PCT * (OCHRONA_MNOZNIK if ochrona_strony(ochrona, sd) else 1)
        e['ryzyko_pct'] = rnd((e['setupy_AB'] + e['pozycje']) * ryz, 2)
        e['za_duze'] = e['ryzyko_pct'] > MAX_RYZYKO_KIERUNEK_PCT
        e['wybrane'] = wybor_ryzyka(rank_rows, sd, ryz, e['pozycje_z_ryzykiem'])   # V6.46: tylko pozycje z ryzykiem
    e_blok = limit_ekspozycji(live)                     # V6.46: DO GRY tylko w limicie ryzyka na kierunek
    if e_blok:
        print(f'Limit ekspozycji: {e_blok} setupow DO GRY poza limitem {MAX_RYZYKO_KIERUNEK_PCT}% na kierunek -> OBSERWACJA')
    rk_df = write_csv('ranking.csv', rank_rows, RANK_FIRST)
    write_csv('ranking_market.csv', [r for r in rank_rows if r['entry_type'] == 'MARKET'], RANK_FIRST, cols=rk_df.columns)
    write_csv('ranking_limit.csv', [r for r in rank_rows if r['entry_type'] == 'LIMIT'], RANK_FIRST, cols=rk_df.columns)
    live.sort(key=lambda r: (order.get(r['status_bramki'], 2), r['filtr'] != '', not r['tradable'], -r['score_R'],
                             r['dist_entry_atr']))
    for pos in positions:
        if not any(r['symbol'] == pos['symbol'] and r['side'] == pos['side'] for r in pos_rows):
            pos_rows.append({'symbol': pos['symbol'], 'side': pos['side'], 'akcja': 'BRAK DANYCH',
                             'powody': 'kontrakt nie pobrany albo za malo swiec'})
    if positions:
        write_csv('pozycje.csv', pos_rows, POS_FIRST)
    all_df = write_csv('setups_all.csv', live, SETUP_FIRST)
    if drive_ok and not replay and live:                # V6.61: dziennik arkusza zlecen do pozniejszego testu
        try:
            kl = pd.DataFrame([{k: r.get(k) for k in ['snapshot_utc', 'symbol', 'setup_type', 'typ_wersja', 'side', 'entry_type',
                                                       'entry', 'zone_ref', 'sl', 'tp1', 'ocena', 'status_setupu', 'price']
                                + KS_KOLUMNY} for r in live])
            kl = add_event_ids(kl)                      # V6.63: ta sama okazja w kolejnych uruchomieniach
            kl = kl[['okazja_id', 'event_id'] + [c for c in kl.columns if c not in ('okazja_id', 'event_id')]]
            dopisz_log(Path(DRIVE_DIR) / KSIAZKA_LOG, kl)
        except Exception as exc:
            print(f'Dziennik arkusza: blad ({str(exc)[:100]})')
    odrz = pd.DataFrame(_REJ['rows'])                   # V6.15: wykryte, ale odrzucone przy budowie
    if len(odrz):
        odrz = odrz.drop_duplicates()
        odrz['powod_grupa'] = odrz['powod'].str.replace(r' \(.*\)$', '', regex=True).str.replace(r' [\d.]+ < [\d.]+$', ' < min', regex=True)
    write_csv('odrzucone.csv', odrz.to_dict('records'), ('symbol', 'side', 'setup_type', 'entry_type', 'powod'))
    try:                                                # V6.62: liderzy 24h i dlaczego bez setupu
        write_csv('movers.csv', movers_rows(summary, live, odrz), MOVERS_COLS)
    except Exception as exc:
        print(f'movers.csv: blad ({str(exc)[:100]})')
    good = [r for r in live if (not r['filtr'] and r['tradable']) or (r.get('tradfi_bez_blokad') and r.get('do_handlu'))]   # V6.92
    write_csv('setups.csv', good, SETUP_FIRST, cols=all_df.columns)
    write_csv('setups_market.csv', [r for r in good if r['entry_type'] == 'MARKET'], SETUP_FIRST, cols=all_df.columns)
    write_csv('setups_limit.csv', [r for r in good if r['entry_type'] == 'LIMIT'], SETUP_FIRST, cols=all_df.columns)
    # V6.104: rankingi diagnostyczne obejmuja takze OBSERWACJE; nie zmieniaja do_handlu.
    # Twarde odrzucenia sa WYKLUCZONE z trzech TOP i pozostaja w setups_all.csv.
    def _ranking_status(r):
        if bool(r.get('do_handlu')) and not r.get('blokady_bezpieczenstwa') and not r.get('blokady_wykonania_tradfi'):
            return 'DOPUSZCZONY'
        return 'OBSERWACJA'

    def _abc_distance_from_row(r):
        """Odleglosc ceny od C w ATR; NIE mylic z dystansem od uniewaznienia.

        Zapasowo odtwarzamy ATR z istniejacego pomiaru odleglosci od
        uniewaznienia. Nie mozna przypisac SWIEZY, gdy brakuje danych.
        """
        def num(v):
            try:
                x = float(v)
                return x if np.isfinite(x) else None
            except (TypeError, ValueError):
                return None
        direct = num(r.get('elliott_abc_od_c_atr'))
        if direct is not None and direct >= 0:
            return direct
        price = num(r.get('price'))
        c = num(r.get('elliott_abc_c'))
        inv = num(r.get('elliott_abc_uniewaznienie'))
        inv_atr = num(r.get('elliott_abc_odleglosc_atr'))
        if None in (price, c, inv, inv_atr) or inv_atr <= 0 or abs(price - inv) <= 0:
            return None
        atr_ = abs(price - inv) / inv_atr
        if not np.isfinite(atr_) or atr_ <= 0:
            return None
        return abs(price - c) / atr_

    def _rank_rows(rows, category):
        oceny = {'A': 0, 'B': 1, 'C': 2, 'D': 3}
        out = []
        for r in rows:
            t = dict(r)
            t.update(status_przewagi_oos_v149(r.get('ocena'), bool(r.get('do_handlu'))))
            t['ranking_kategoria'] = category
            # Nie deduplikuj odmiennych strategii na tym samym instrumencie.
            # ID czytelne takze w CSV z powtorzonym symbolem.
            t['ranking_setup_id'] = ' | '.join(str(r.get(k) or '-') for k in
                ('symbol', 'setup_type', 'side', 'entry_type'))
            t['ranking_status'] = _ranking_status(r)
            t['ranking_powod'] = (r.get('warunki') or r.get('not_tradable_reason') or
                                  r.get('blokady_kwalifikacji') or r.get('filtr') or '')
            t['ranking_charakter'] = 'BADAWCZY_NIE_JEST_SYGNALEM' if category == 'ELLIOTT' else 'SETUP_Z_ISTNIEJACYCH_FILTROW'
            t['ranking_jakosc'] = str(r.get('ocena') or 'BRAK')
            t['ranking_wykonalnosc'] = ('TECHNICZNIE_DOPUSZCZONY_NIE_POTWIERDZA_PRZEWAGI'
                                        if t['ranking_status'] == 'DOPUSZCZONY' else 'NIE_DOPUSZCZONY')
            if category == 'ELLIOTT':
                dist = _abc_distance_from_row(r)
                if dist is not None:
                    t['elliott_abc_od_c_atr'] = round(dist, 2)
                # Priorytet ma mierzalny dystans C, a nie potencjalnie puste pole statusu.
                old_f = str(r.get('elliott_abc_swiezosc') or 'BRAK_DANYCH')
                if dist is None:
                    f = 'NIEZWERYFIKOWANY'
                elif dist > 2.5 or old_f == 'ODDALONY':
                    f = 'ODDALONY'
                elif old_f == 'SWIEZY':
                    f = 'SWIEZY'
                else:
                    f = 'DO_OBSERWACJI'
                t['elliott_abc_swiezosc'] = f
                t['elliott_abc_spozniony'] = f == 'ODDALONY'
                t['elliott_abc_status_rankingu'] = ('SPOZNIONY_ABC' if f == 'ODDALONY'
                                                  else 'SWIEZY_ABC' if f == 'SWIEZY'
                                                  else 'ABC_BRAK_POMIARU' if f == 'NIEZWERYFIKOWANY'
                                                  else 'ABC_DO_OBSERWACJI')
            out.append(t)
        out.sort(key=lambda r: (r['ranking_status'] != 'DOPUSZCZONY',
                                r.get('elliott_abc_swiezosc') != 'SWIEZY' if category == 'ELLIOTT' else False,
                                r.get('elliott_abc_zgodnosc_4h') != 'ZGODNE' if category == 'ELLIOTT' else False,
                                oceny.get(str(r.get('ocena')), 4),
                                -float(r.get('rr_tp1_net') or 0), str(r.get('symbol') or '')))
        for i, r in enumerate(out, 1):
            r['ranking_pozycja'] = i
        return out

    extra_cols = ['ranking_setup_id', 'ranking_kategoria', 'ranking_pozycja', 'ranking_status', 'ranking_powod',
                  'ranking_charakter', 'ranking_jakosc', 'ranking_wykonalnosc',
                  'dopuszczenie_techniczne', 'przewaga_oos_potwierdzona', 'status_sygnalu_v149',
                  'ostrzezenie_ocena_d_v149', 'uwaga_przewaga',
                  'elliott_abc_spozniony', 'elliott_abc_status_rankingu',
                  'elliott_abc_swiezosc', 'elliott_abc_wiek_h',
                  'elliott_abc_od_c_atr', 'elliott_abc_c_utc_ms']
    extra_cols += ['elliott_okazja_id_v158','elliott_geometria_sl_v158','elliott_kwalifikacja_v158','elliott_status_strategii_bazowej_v158']
    out_cols = extra_cols + ['elliott_status_kandydata_v156','elliott_rr_brutto_v156','model_celow_v156'] + list(all_df.columns)
    market_rank = _rank_rows([r for r in good if r.get('entry_type') == 'MARKET'], 'MARKET')
    limit_rank = _rank_rows([r for r in good if r.get('entry_type') == 'LIMIT'], 'LIMIT')
    ell_candidates = [dict(r) for r in good if r.get('elliott_abc_status') == 'ABC_POTENCJALNIE_ZAKONCZONE'
                      and r.get('elliott_abc_kierunek') == r.get('side')]
    for r in ell_candidates:
        r['elliott_zrodlo_wejscia'] = r.get('entry_type')
        r['elliott_rola'] = 'BADAWCZY_ABC_BEZ_OSOBNEGO_WEJSCIA'
    ell_rank = _rank_rows(ell_candidates, 'ELLIOTT')
    for r in ell_rank:
        r['elliott_status_strategii_bazowej_v158'] = r.get('ranking_status')
        r['elliott_geometria_sl_v158'] = 'NIE_DOTYCZY_STRATEGIA_BAZOWA'
        r['elliott_kwalifikacja_v158'] = ('SWIEZY_ZGODNY_4H_BAZOWY_SETUP'
            if r.get('elliott_abc_swiezosc') == 'SWIEZY' and r.get('elliott_abc_zgodnosc_4h') == 'ZGODNE'
            else 'ABC_NIE_ZWERYFIKOWANE_DO_TOP')
        r['elliott_okazja_id_v158'] = str(r.get('symbol'))+'|'+str(r.get('side'))+'|'+str(r.get('elliott_abc_c_utc_ms') or snap_iso(now_ms))
        # Dopuszczenie strategii bazowej NIE jest dopuszczeniem Elliotta.
        r['ranking_status'] = 'OBSERWACJA_ELLIOTT'
        r['ranking_wykonalnosc'] = 'BADAWCZY_BEZ_DOPUSZCZENIA_ELLIOTT'

    # Osobne hipotezy ABC nie potrzebuja zgodnego setupu innej strategii.
    # TYLKO raport badawczy. Nie dopisujemy ich do live/good/gate/proba.
    ell_wlasne = elliott_wlasne_abc_v156(summary, snap_iso(now_ms))
    for rec in ell_wlasne:
        rec['ranking_pozycja'] = len(ell_rank)+1
        ell_rank.append(rec)
    # Historyczny backtest autonomicznej strategii ABC: osobny, tylko badawczy.
    # Nie trafia do good, gate, PROBA ani do_handlu.
    if BACKTEST:
        abc_bt159, abc_meta159 = elliott_backtest_abc_v159(PP, now_ms)
    else:
        abc_bt159, abc_meta159 = [], {'status':'NIE_URUCHOMIONO_BACKTEST_OFF'}
    # Siatki porownawcze sa niezaleznymi symulacjami. Nie sumujemy okazji miedzy siatkami.
    abc_siats_v160 = {12:abc_bt159}
    abc_metas_v160 = {12:abc_meta159}
    if BACKTEST:
        for _step in (6,3):
            _rows, _meta = elliott_backtest_abc_v159(PP, now_ms, step_h=_step)
            abc_siats_v160[_step] = _rows
            abc_metas_v160[_step] = _meta
    abc_all_v160 = []
    abc_groups_v160 = []
    for _step, _rows in abc_siats_v160.items():
        abc_all_v160.extend([dict(r, krok_h=_step) for r in _rows])
        abc_groups_v160.extend(elliott_podsumowanie_v160(_rows, _step))
    abc_kontrola_v161 = []
    for _step, _rows in abc_siats_v160.items():
        for _market in ('CRYPTO','TRADFI','RAZEM'):
            _subset = [r for r in _rows if _market=='RAZEM' or r.get('rynek_v160')==_market]
            _cnt = elliott_statusy_v161(_subset)
            _summary = next(z for z in abc_groups_v160 if z['krok_h']==_step and z['rynek']==_market and z['grupa']=='WSZYSTKIE')
            assert _summary['kandydatury']==_cnt['kandydatury']
            assert _summary['rozliczone']==_cnt['rozliczone']
            abc_kontrola_v161.append({'krok_h':_step,'rynek':_market, **_cnt,'kontrola':'ZGODNE'})
    abc_unique162, abc_threshold162, abc_wf162, abc_meta162 = abc_v162_analiza(abc_all_v160)
    write_csv('elliott_abc_unikalne_v162.csv', abc_unique162,
              ['unikalny_abc_id_v162','snapshot_utc','symbol','side','elliott_abc_c_utc_ms','kanoniczny_krok_h_v162','rynek_v160','status','r_netto','koszt_r_est_v160'])
    write_csv('elliott_abc_progi_koszt_v162.csv', abc_threshold162,
              ['rynek','prog_r','populacja','wszystkie_unikalne','wszystkie_rozliczone_z_kosztem','liczba','sredni_R_netto','sredni_koszt_R_est','sredni_R_plus_koszt_proxy'])
    write_csv('elliott_abc_walk_forward_v162.csv', abc_wf162,
              ['rynek','zbior','podzial_utc','wybrany_prog_r','prog_r','liczba','sredni_R_netto','status'])
    save_json('elliott_abc_metoda_v162.json',abc_meta162)
    abc_wf163, abc_meta163 = abc_v163_porownanie(abc_all_v160)
    write_csv('elliott_abc_walk_forward_porownanie_v163.csv',abc_wf163,
              ['rynek','zbior','podzial_utc','status','prog_wybrany_train_r','wszystkie_sygnaly','rozliczone_z_kosztem',
               'bez_filtra_n','bez_filtra_suma_R','bez_filtra_sredni_R','z_filtrem_n','z_filtrem_suma_R',
               'z_filtrem_sredni_R','odrzucone_przez_filtr_n','odrzucone_suma_R','odrzucone_sredni_R',
               'zmiana_sredniego_R_vs_bez_filtra','zmiana_sumy_R_vs_bez_filtra'])
    save_json('elliott_abc_walk_forward_metoda_v163.json',abc_meta163)
    abc_wf164, abc_meta164 = abc_v164_wf(abc_all_v160)
    write_csv('elliott_abc_walk_forward_wiele_okien_v164.csv',abc_wf164,
              ['fold','rynek','cut_utc','test_koniec_utc','status','wariant','prog_r','train_n',
               'test_n','test_wybrane_n','odrzucone_n','portfel_n','portfel_bez_slotu_n',
               'dodatkowy_roundtrip_bps','wynik_n','suma_R','sredni_R'])
    save_json('elliott_abc_walk_forward_metoda_v164.json',abc_meta164)
    abc_wf165, abc_events165, abc_meta165 = abc_v165_wf(abc_all_v160)
    write_csv('elliott_abc_portfel_wf_v165.csv',abc_wf165,
              ['fold','rynek','wariant','cut_utc','test_koniec_utc','prog_r','train_n',
               'sygnaly','odrzucone_koszt','odrzucone_sloty','wybrane_rozliczone',
               'wybrane_nierozliczone','suma_R','sredni_R','maksimum_slotow','status'])
    write_csv('elliott_abc_portfel_zdarzenia_v165.csv',abc_events165,
              ['fold','rynek_analizy','wariant','abc_id','snapshot_utc','decyzja',
               'koszt_r','status','exit_utc','r_netto','aktywne_przed'])
    save_json('elliott_abc_portfel_metoda_v165.json',abc_meta165)
    abc_wf166, abc_events166, abc_occupancy166, abc_meta166, abc_events_all167 = abc_v166_wf(abc_all_v160)
    write_csv('elliott_abc_portfel_ciagly_wf_v166.csv', abc_wf166,
              ['rynek','fold','wariant','cut_utc','test_koniec_utc','prog_r','sygnaly','wybrane',
               'odrzucone_koszt','odrzucone_sloty','bez_progu','aktywnych_na_poczatku',
               'zamkniecia_w_oknie','zamkniecia_z_poprzednich_okien','rozliczone_oos','suma_R','sredni_R','maksimum_slotow'])
    write_csv('elliott_abc_portfel_ciagly_zdarzenia_v166.csv', abc_events166,
              ['rynek_analizy','wariant','fold','abc_id','snapshot_utc','decyzja','aktywne_przed','aktywne_po','koszt_r','status','exit_utc'])
    write_csv('elliott_abc_zajetosc_granic_v166.csv',abc_occupancy166,
              ['rynek','wariant','fold','cut_utc','aktywnych_na_granicy','abc_id_aktywnych'])
    save_json('elliott_abc_portfel_ciagly_metoda_v166.json',abc_meta166)
    abc_positions167, abc_boundaries167, abc_controls167 = abc_v167_audyt(
        abc_events_all167, abc_occupancy166, abc_wf166, V164_PORTFOLIO_SLOTS)
    write_csv('elliott_abc_pozycje_przenoszone_v167.csv',abc_positions167,
              ['rynek','wariant','abc_id','wejscie_utc','wyjscie_utc','fold_wejscia','fold_wyjscia',
               'pochodzenie','status_zrodla','rozliczona','r_netto','pnl_do_oos',
               'przeniesiona_miedzy_oknami','pozostaje_otwarta'])
    write_csv('elliott_abc_granice_audyt_v167.csv',abc_boundaries167,
              ['rynek','wariant','fold','cut_utc','aktywnych_niezaleznie','aktywnych_raport',
               'abc_id_aktywnych','kontrola'])
    write_csv('elliott_abc_portfel_integralnosc_v167.csv',abc_controls167,
              ['rynek','wariant','fold','rozliczone_oos','rozliczone_oos_raport',
               'rozgrzewka_zamknieta','wszystkie_zamkniecia','suma_R_oos','suma_R_raport',
               'maksimum_slotow_niezaleznie','maksimum_slotow_raport','kontrola'])
    save_json('elliott_abc_audyt_metoda_v167.json',{
        'wersja':'V6.169','status':'BADAWCZY','pozycje':len(abc_positions167),
        'kontrole_granic':len(abc_boundaries167),'kontrole_portfela':len(abc_controls167),
        'uwagi':['Kontrola przedzialow z czasem wejscia i zamkniecia, bez znajomosci fill gieldowego',
                 'Rozgrzewka rezerwuje sloty, lecz jej wynik nie wchodzi do OOS',
                 'R przypisywany do okna wyjscia, pozycje nierozliczone bez PnL',
                 'Wyniki nie zmieniaja live gate ani do_handlu']})


    write_csv('elliott_abc_kontrola_statusow_v161.csv', abc_kontrola_v161,
              ['krok_h','rynek','kandydatury','rozliczone','otwarte','oczekujace','inne_nierozliczone','brak_danych','kontrola'])
    write_csv('elliott_abc_siatki_v160.csv', abc_all_v160,
              ['krok_h','snapshot_utc','symbol','asset_type','rynek_v160','side',
               'entry_snapshot','sl','tp1','status','r_netto','slip_bps',
               'koszt_r_est_v160','ekonomika_v160'])
    write_csv('elliott_abc_rynki_koszt_v160.csv', abc_groups_v160,
              ['krok_h','rynek','grupa','kandydatury','rozliczone','otwarte','oczekujace','inne_nierozliczone','brak_danych','sredni_R','suma_R','odsetek_zysku','status'])
    save_json('elliott_abc_metoda_v160.json', {
        'wersja':'V6.161','kroki_h':list(abc_siats_v160),
        'liczebnosc':{str(k):len(v) for k,v in abc_siats_v160.items()},
        'koszt_r_est':'Orientacyjny round-trip taker/taker: (2*slip_bps + 2*taker_fee_bps)/10000 / (abs(entry-sl)/entry). Bez fill/partial; nie jest to dokładna egzekucja.',
        'prog_diagnostyczny_r':0.5,
        'typy':'COIN=CRYPTO, inne=TRADFI',
        'uwagi':['osobne populacje dla kazdego kroku; brak sumowania wynikow',
                 'kroki 3/6/12h maja zaleznie skorelowane i czesciowo pokrywajace sie okazje',
                 'brak niezaleznego OOS i historycznego orderbooka',
                 'koszt/R jest ostrzezeniem, nie filtrem aktywnego handlu',
                 'nie zmienia gate.csv, do_handlu ani ryzyka']})
    write_csv('elliott_abc_backtest_v159.csv', abc_bt159,
              ['snapshot_utc','symbol','side','entry_snapshot','sl','tp1','tp2_badawcze',
               'slip_bps','status','filled','r_netto','czesc_1R','fill_utc','exit_utc','luka_danych','uwaga'])
    _final159 = [r for r in abc_bt159 if r.get('status') in FINAL and not r.get('luka_danych')]
    vals159 = [float(r['r_netto']) for r in _final159 if r.get('r_netto') is not None]
    abc_meta159.update(kandydatury=len(abc_bt159), rozliczone=len(vals159),
        sredni_R=(sum(vals159)/len(vals159) if vals159 else None),
        suma_R=(sum(vals159) if vals159 else None),
        nierozliczone=sum(r.get('status') not in FINAL for r in abc_bt159),
        warning='Nie interpretowac historycznych sygnalow jako potwierdzonej przewagi OOS')
    save_json('elliott_abc_backtest_metoda_v159.json',abc_meta159)
    print('Elliott ABC v159 (badawczo):', abc_meta159['kandydatury'],
          'kandydatur,',abc_meta159['rozliczone'],'rozliczonych; bez zmiany bramek')
    write_csv('elliott_wlasne_abc_v158.csv', ell_wlasne,
              ['symbol','side','entry','sl','tp1','tp2','elliott_abc_swiezosc',
               'elliott_geometria_sl_v158','elliott_kwalifikacja_v158','elliott_okazja_id_v158',
               'elliott_status_kandydata_v156','do_handlu','ranking_status'])
    write_csv('elliott_wlasne_abc_v157.csv', ell_wlasne,
              ['symbol','side','entry','sl','tp1','tp2','elliott_atr_1h_v157','elliott_abc_c_utc_ms','elliott_abc_wiek_h','elliott_abc_od_c_atr','elliott_abc_swiezosc','elliott_rr_brutto_v156',
               'elliott_status_kandydata_v156','do_handlu','ranking_status'])
    write_csv('ranking_market_status.csv', market_rank, out_cols, cols=out_cols)
    write_csv('ranking_limit_status.csv', limit_rank, out_cols, cols=out_cols)
    write_csv('setups_elliott.csv', ell_rank, SETUP_FIRST,
              cols=out_cols + ['elliott_zrodlo_wejscia', 'elliott_rola'])
    write_csv('ranking_elliott.csv', ell_rank, RANK_FIRST,
              cols=out_cols + ['elliott_zrodlo_wejscia', 'elliott_rola'])
    # Stara lista top_setups.csv pozostaje NIEZMIENIONA: surowe kryteria przewagi strategii.
    top = top_setups(good)
    write_csv('top_setups.csv', top, TOP_FIRST)
    # Trzy widoczne rankingi pokazują także obserwacje, ale NIGDY nie znoszą blokad.
    write_csv('top_market.csv', market_rank[:10], out_cols, cols=out_cols)
    write_csv('top_limit.csv', limit_rank[:10], out_cols, cols=out_cols)
    # Spóźnione ABC pozostają w pełnym raporcie, ale nie trafiają do TOP okazji.
    # SWIEZY i DO_OBSERWACJI nie sa rownowazne. TOP zawiera tylko potwierdzony status.
    # W TOP jedna pozycja na jedna niezalezna okazje (symbol+kierunek+punkt C).
    # Pierwszenstwo: samodzielna hipoteza z poprawna geometria; bazowy setup to alternatywne wejscie.
    _ell_top_candidates = [r for r in ell_rank if r.get('elliott_kwalifikacja_v158') in
                           ('SWIEZY_ZGODNY_4H_CENY_POPRAWNE', 'SWIEZY_ZGODNY_4H_BAZOWY_SETUP')]
    _ell_top_candidates.sort(key=lambda r: (
        r.get('elliott_rola') != 'SAMODZIELNY_KANDYDAT_BADAWCZY',
        str(r.get('symbol') or ''), str(r.get('side') or '')))
    ell_aktualne = []
    _elliott_seen_v158 = set()
    for r in _ell_top_candidates:
        _key = (r.get('symbol'), r.get('side'), r.get('elliott_abc_c_utc_ms'))
        if _key in _elliott_seen_v158:
            continue
        _elliott_seen_v158.add(_key)
        ell_aktualne.append(r)
    write_csv('elliott_unikalne_okazje_v158.csv', ell_aktualne,
              ['symbol','side','entry','sl','tp1','tp2','elliott_okazja_id_v158',
               'elliott_kwalifikacja_v158','elliott_status_strategii_bazowej_v158','ranking_status',
               'do_handlu'], cols=out_cols + ['elliott_okazja_id_v158','elliott_kwalifikacja_v158',
               'elliott_status_strategii_bazowej_v158','elliott_geometria_sl_v158'])
    ell_obserwacyjne = [r for r in ell_rank if r.get('elliott_abc_swiezosc') == 'DO_OBSERWACJI']
    ell_spoznione = [r for r in ell_rank if r.get('elliott_abc_spozniony')]
    ell_niezweryfikowane = [r for r in ell_rank if r.get('elliott_abc_swiezosc') == 'NIEZWERYFIKOWANY']
    write_csv('top_elliott.csv', ell_aktualne[:10], out_cols + ['elliott_zrodlo_wejscia', 'elliott_rola'],
              cols=out_cols + ['elliott_zrodlo_wejscia', 'elliott_rola'])
    write_csv('elliott_abc_obserwacja.csv', ell_obserwacyjne, out_cols + ['elliott_zrodlo_wejscia', 'elliott_rola'],
              cols=out_cols + ['elliott_zrodlo_wejscia', 'elliott_rola'])
    write_csv('elliott_spoznione.csv', ell_spoznione, out_cols + ['elliott_zrodlo_wejscia', 'elliott_rola'],
              cols=out_cols + ['elliott_zrodlo_wejscia', 'elliott_rola'])
    write_csv('elliott_niezweryfikowane.csv', ell_niezweryfikowane, out_cols + ['elliott_zrodlo_wejscia', 'elliott_rola'],
              cols=out_cols + ['elliott_zrodlo_wejscia', 'elliott_rola'])
    # Osobny widok A-C: status dopuszczenia to nie jakosc strategii.
    write_csv('top_market_jakosc.csv', [r for r in market_rank if r.get('ranking_jakosc') in ('A','B','C')][:10], out_cols, cols=out_cols)
    write_csv('top_limit_jakosc.csv', [r for r in limit_rank if r.get('ranking_jakosc') in ('A','B','C')][:10], out_cols, cols=out_cols)
    print(f'WYNIKI {VERSION} MARKET={len(market_rank)}, LIMIT={len(limit_rank)}, '
          f'ELLIOTT ABC={len(ell_rank)} (TOP bez opoznionych: {len(ell_aktualne)}, '
          f'obserwacja: {len(ell_obserwacyjne)}, spoznione: {len(ell_spoznione)}, bez pomiaru: {len(ell_niezweryfikowane)}); dopuszczone: '
          f'{sum(r["ranking_status"] == "DOPUSZCZONY" for r in market_rank + limit_rank)}; '
          'bramki bezpieczeństwa bez zmian')
    summary.sort(key=lambda r: -(r['quote_vol_24h'] or 0))
    write_csv('summary.csv', summary, SUMMARY_FIRST)
    save_json('summary.json', summary)
    qc_info = {k: v for k, v in qc.items() if isinstance(v, dict) and set(v) <= {'oi_niewiarygodne', 'price_mismatch_pct'}
               and (v.get('price_mismatch_pct') or 0) <= PRICE_MISMATCH_PCT}   # V6.30: informacja, nie problem
    # V6.112: audyt per symbol/interwal; brak lub odrzucenie rekordow sam w sobie
    # NIE oznacza, ze analizowane okno jest niekompletne. Nie dotyka bramek handlowych.
    historia_problemy = []
    historia_kontrola = []
    for sym, P in PP.items():
        A = view(P)
        raw_issues = qc.get(sym, {})
        repair_gaps = gaps_qc(P)
        symbol_enough = enough(A)
        for tf, min_bars in MIN_BARS.items():
            arr = A.get(tf)
            available = len(arr) if arr is not None else 0
            repair = _history_repairs.get((sym, tf), {})
            gaps = int(repair_gaps.get(tf, 0))
            short = available < min_bars
            bad_raw = raw_issues.get('zle_swiece_usuniete', {})
            bad_value = bad_raw.get(tf, 0) if isinstance(bad_raw, dict) else bad_raw
            try:
                bad_count = max(0, int(bad_value or 0))
            except (TypeError, ValueError, OverflowError):
                bad_count = 0
            bad = bad_count > 0
            ms = INTERVAL_MS[tf]
            # Ostatnia OCZEKIWANA zamknieta swieca ma open=cur-ms.
            # Market TradFi moze byc zamkniety: to sygnal diagnostyczny, nie
            # samodzielna zmiana zasad wykonania/transakcji.
            expected = (now_ms // ms) * ms - ms
            # Audyt dotyczy ostatniej ZAMKNIETEJ swiecy. view(P) moze
            # zawierac otwarta swiece o czasie (now_ms // ms) * ms;
            # nie wolno klasyfikowac jej jako spoznionej ani uznawac za zamknieta.
            newest_raw = int(arr[-1, T]) if available else None
            closed_times = arr[:, T]
            closed_times = closed_times[closed_times <= expected]
            latest = int(closed_times[-1]) if len(closed_times) else None
            age_bars = max(0, (expected - latest) // ms) if latest is not None else None
            fresh = latest is not None and 0 <= expected - latest < ms
            open_bar_present = newest_raw is not None and newest_raw > expected
            continuity = gaps == 0
            # Blocker odnosi sie do TEGO interwalu i wymagan analizy live;
            # podsumowanie dla calego symbolu jest osobne.
            block_reasons = []
            if short:
                block_reasons.append('ZA_MALO_SWIEC')
            if not continuity:
                block_reasons.append('LUKI_W_OKNIE')
            if not fresh:
                block_reasons.append('NIESWIEZE_LUB_BRAK_SWIEC')
            if repair.get('blad') and not available:
                block_reasons.append('BLAD_API_BRAK_DANYCH')
            blocked = bool(block_reasons)
            status = ('NATURALNIE_KROTKA_POTWIERDZONA' if short and (sym, tf) in _short_history_verified
                      else 'KROTKA_NIEPOTWIERDZONA' if short
                      else 'LUKI_W_HISTORII' if gaps
                      else 'NIESWIEZE_SWIECE' if not fresh
                      else 'ODRZUCONE_SWIECE' if bad
                      else 'BLAD_API' if repair.get('blad') else 'OK')
            item = {
                'symbol': sym, 'interwal': tf, 'status': status,
                'swiece_wymagane': min_bars, 'swiece_dostepne': available,
                'brakuje': max(0, min_bars - available), 'luki_pozostale': gaps,
                'swiezosc_ok': bool(fresh), 'ciaglosc_ok': bool(continuity),
                'ostatnia_swieca_utc': iso(latest) if latest is not None else '',
                'ostatnia_swieca_surowa_utc': iso(newest_raw) if newest_raw is not None else '',
                'otwarta_swieca_pominieta': bool(open_bar_present),
                'oczekiwana_swieca_utc': iso(expected),
                'opoznienie_interwalow': int(age_bars) if age_bars is not None else '',
                'blokada_powod': ';'.join(block_reasons),
                'powod_pobrania': repair.get('powod', ''),
                'swiece_zadane': repair.get('zadano', 0), 'swiece_otrzymane': repair.get('otrzymano', 0),
                'swiece_odrzucone': bad_count,
                'blad': repair.get('blad', ''),
                'blokuje_analize': blocked,
                'symbol_ma_wymagane_swiece': bool(symbol_enough),
            }
            historia_kontrola.append(item)
            if short or gaps or bad or repair.get('blad') or not fresh:
                historia_problemy.append(item)
    historia_cols = ['symbol','interwal','status','swiece_wymagane','swiece_dostepne','brakuje',
                     'luki_pozostale','swiezosc_ok','ciaglosc_ok','ostatnia_swieca_utc',
                     'oczekiwana_swieca_utc','opoznienie_interwalow',
                     'ostatnia_swieca_surowa_utc','otwarta_swieca_pominieta','blokada_powod',
                     'powod_pobrania','swiece_zadane','swiece_otrzymane',
                     'swiece_odrzucone','blad','blokuje_analize','symbol_ma_wymagane_swiece']
    write_csv('historia_problemy.csv', historia_problemy, historia_cols, cols=historia_cols)
    save_json('historia_problemy.json', historia_problemy)
    # W pelnym pliku widac rowniez poprawne interwaly - przydatne do kontroli false-positive.
    write_csv('historia_kontrola.csv', historia_kontrola, historia_cols, cols=historia_cols)
    # V6.115: zrodlo i przyklady rekordow odrzuconych w prepare().
    # Nie uznajemy ujemnego wolumenu czy blednego czasu za odrzucenie, jesli
    # nie stanowily warunku filtrowania w dotychczasowej implementacji.
    diagnostyka_swiec = []
    for sym, P in PP.items():
        for tf, info in P.get('diagnostyka_odrzucen', {}).items():
            for reason, count in info['powody'].items():
                diagnostyka_swiec.append({'symbol': sym, 'interwal': tf,
                                         'powod': reason, 'liczba': int(count),
                                         'przyklady_json': json.dumps(
                                             [e for e in info['przyklady'] if e['powod'] == reason],
                                             ensure_ascii=False, allow_nan=False)})
    diag_cols = ['symbol', 'interwal', 'powod', 'liczba', 'przyklady_json']
    write_csv('historia_przyczyny_odrzucen.csv', diagnostyka_swiec, diag_cols, cols=diag_cols)
    save_json('historia_przyczyny_odrzucen.json', diagnostyka_swiec, pretty=True)
    # V6.116: pochodzenie odczytanych zerowych rekordow. Brak obserwacji w
    # ARCHIWUM nie dowodzi, ze ich nigdy nie bylo: archiwum zapisuje oczyszczone swiece.
    zerowe_zrodla = zero_provenance_rows()
    zero_cols = ['symbol','interwal','zrodlo','liczba_zerowych_swiec',
                 'liczba_przedzialow','przedzialy_json','przedzialy_pominiete']
    write_csv('historia_zerowe_zrodla.csv', zerowe_zrodla, zero_cols, cols=zero_cols)
    save_json('historia_zerowe_zrodla.json', zerowe_zrodla, pretty=True)
    zero_summary = {'api_zerowe': sum(r['liczba_zerowych_swiec'] for r in zerowe_zrodla if r['zrodlo']=='API'),
                    'archiwum_zerowe': sum(r['liczba_zerowych_swiec'] for r in zerowe_zrodla if r['zrodlo']=='ARCHIWUM'),
                    'uwaga': 'Archiwum przechowuje oczyszczone swiece; brak zer w archiwum nie wyklucza ich historycznej obecnosci.'}
    save_json('historia_zerowe_podsumowanie.json', zero_summary, pretty=True)
    # V6.117: jedno wydarzenie dla zachodzacych na siebie okresow 5m/15m/1h/4h.
    zero_events, zero_matches, zero_events_summary = zero_event_diagnostics()
    event_cols = ['symbol','od_utc','do_utc_wylacznie','czas_h','interwaly',
                  'zrodla','rekordy_interwalowe','odrebne_zdarzenie','us30']
    match_cols = ['symbol','interwal','api_zerowe','archiwum_zerowe',
                  'wspolne_otwarcia','tylko_api','tylko_archiwum',
                  'przypadki_wspolne_json','uwaga']
    write_csv('historia_zerowe_zdarzenia.csv', zero_events, event_cols, cols=event_cols)
    write_csv('historia_zerowe_api_archiwum.csv', zero_matches, match_cols, cols=match_cols)
    save_json('historia_zerowe_zdarzenia.json', zero_events, pretty=True)
    save_json('historia_zerowe_us30.json', {
        'symbol': 'US30USDT', 'zdarzenia': [e for e in zero_events if e['us30']],
        'porownania_api_archiwum': [e for e in zero_matches if e['symbol'].upper() == 'US30USDT'],
        'uwaga': 'Brak notowan lub wadliwe dane: sam zapis zer nie rozstrzyga przyczyny.'
    }, pretty=True)
    save_json('historia_zerowe_zdarzenia_podsumowanie.json', zero_events_summary, pretty=True)
    # V6.118: kontrolowany odczyt historyczny; tylko raport, nigdy dane do analiz.
    zero_verify_rows, zero_verify_summary = verify_us30_historical_zeros(replay=replay)
    zero_verify_cols = ['symbol','interwal','punkt_kontrolny_utc',
                        'archiwum_zerowych_obserwowanych','api_biezace_zerowych',
                        'api_liczba_swiec','wspolne_czasy','zero_w_api','niezero_w_api',
                        'brak_w_api','status','uzasadnienie','zrodlo_porownania',
                        'api_od_utc','api_do_utc','proby_zakresu','historyczny_zakres_pokryty','status_jakosci_zrodla']
    write_csv('historia_zerowe_us30_weryfikacja.csv', zero_verify_rows,
              zero_verify_cols, cols=zero_verify_cols)
    save_json('historia_zerowe_us30_weryfikacja.json',
              {'wyniki':zero_verify_rows,'podsumowanie':zero_verify_summary}, pretty=True)

    # V6.113: oddzielny, opcjonalny snapshot diagnostyczny. Nie jest zrodlem
    # danych rynkowych ani bramka: awaria odczytu/zapisu niczego nie odblokowuje.
    # Klucz zawiera symbol i interwal; porownujemy tylko pary widziane w obu biegach.
    qc_delta = []
    qc_repeat = []
    qc_snapshot_file = Path(DRIVE_DIR) / 'historia_qc_snapshot_v6113.json'
    previous = {}
    qc_snapshot_status = 'BEZ_DYSKU' if not drive_ok else 'PIERWSZE_URUCHOMIENIE'
    if drive_ok and not replay and qc_snapshot_file.is_file():
        try:
            old_obj = json.loads(qc_snapshot_file.read_text(encoding='utf-8'))
            if old_obj.get('schema') == 1 and isinstance(old_obj.get('rows'), dict):
                previous = old_obj['rows']
                qc_snapshot_status = 'POROWNANO'
            else:
                qc_snapshot_status = 'NIEZGODNY_FORMAT'
        except (OSError, ValueError, TypeError) as exc:
            qc_snapshot_status = 'BLAD_ODCZYTU: ' + str(exc)[:120]
    current_rows = {}
    for r in historia_kontrola:
        key = str(r['symbol']) + '|' + str(r['interwal'])
        old = previous.get(key, {}) if isinstance(previous.get(key), dict) else {}
        # Surowe ostrzezenia odroznione od faktycznej blokady live.
        rejected = int(r.get('swiece_odrzucone', 0) or 0) > 0
        old_rejected = bool(old.get('odrzucone', False))
        prev_count = int(old.get('kolejne_odrzucenia', 0) or 0) if old else 0
        streak = prev_count + 1 if rejected and old_rejected else (1 if rejected else 0)
        now = {'status': str(r.get('status','')), 'blokuje_analize': bool(r.get('blokuje_analize')),
               'brakuje': int(r.get('brakuje',0)), 'luki': int(r.get('luki_pozostale',0)),
               'swiezosc_ok': bool(r.get('swiezosc_ok')), 'ciaglosc_ok': bool(r.get('ciaglosc_ok')),
               'odrzucone': rejected, 'kolejne_odrzucenia': streak,
               'swiece_dostepne': int(r.get('swiece_dostepne',0))}
        current_rows[key] = now
        if rejected:
            qc_repeat.append({'symbol': r['symbol'], 'interwal': r['interwal'],
                              'kolejne_uruchomienia_z_odrzuceniami': streak,
                              'status_wykrycia': 'POWTARZA_SIE' if streak >= 2 else 'PIERWSZE_WYKRYCIE',
                              'czy_powtarzalny': streak >= 2,
                              'ostatnie_odrzucone': int(r.get('swiece_odrzucone', 0) or 0),
                              'blokuje_analize': now['blokuje_analize'],
                              'powod_blokady': r.get('blokada_powod','')})
        if old:
            changes = [field for field in ('status','blokuje_analize','brakuje','luki','swiezosc_ok',
                                          'ciaglosc_ok','odrzucone') if old.get(field) != now[field]]
            if changes:
                qc_delta.append({'symbol': r['symbol'], 'interwal': r['interwal'],
                                 'zmienione_pola': ';'.join(changes),
                                 'status_przed': old.get('status',''), 'status_teraz': now['status'],
                                 'blokada_przed': old.get('blokuje_analize',''),
                                 'blokada_teraz': now['blokuje_analize'],
                                 'braki_przed': old.get('brakuje',''), 'braki_teraz': now['brakuje'],
                                 'odrzucone_przed': old_rejected, 'odrzucone_teraz': rejected})
    delta_cols = ['symbol','interwal','zmienione_pola','status_przed','status_teraz',
                  'blokada_przed','blokada_teraz','braki_przed','braki_teraz',
                  'odrzucone_przed','odrzucone_teraz']
    repeat_cols = ['symbol','interwal','kolejne_uruchomienia_z_odrzuceniami',
                   'status_wykrycia','czy_powtarzalny','ostatnie_odrzucone',
                   'blokuje_analize','powod_blokady']
    write_csv('historia_zmiany.csv', qc_delta, delta_cols, cols=delta_cols)
    write_csv('historia_powtarzalne_bledy.csv', qc_repeat, repeat_cols, cols=repeat_cols)
    write_csv('historia_tylko_powtarzalne.csv',
              [r for r in qc_repeat if r['czy_powtarzalny']], repeat_cols, cols=repeat_cols)
    save_json('historia_qc_diagnostyka.json', {'porownanie': qc_snapshot_status,
              'zmienione_interwaly': len(qc_delta), 'odrzucenia_obecnie': len(qc_repeat),
              'pierwsze_wykrycia': sum(not x['czy_powtarzalny'] for x in qc_repeat),
              'powtarzalne_odrzucenia': sum(x['czy_powtarzalny'] for x in qc_repeat)}, pretty=True)
    # V6.176: raport interpretacyjny QC tylko do diagnostyki. Nie zmienia
    # blokad, populacji backtestu, swiec, ocen ani kolejnosci setupow.
    historia_qc_przyczyny_v176 = raport_qc_przyczyn_v176(
        historia_kontrola, qc_repeat, diagnostyka_swiec)
    cols_v176 = ('symbol','interwal','status_historii','powod_odrzucenia',
                 'liczba_odrzuconych','kolejne_uruchomienia','blokuje_analize',
                 'powod_blokady','brakuje_swiec','ocena_diagnostyczna')
    write_csv('historia_qc_przyczyny_v176.csv', historia_qc_przyczyny_v176,
              cols_v176, cols=cols_v176)
    save_json('historia_qc_przyczyny_v176.json', {
        'wersja': VERSION,
        'zrodlo': ['historia_kontrola.csv','historia_powtarzalne_bledy.csv',
                    'historia_przyczyny_odrzucen.csv'],
        'rekordy': historia_qc_przyczyny_v176,
        'uwaga': 'Diagnostyka historycznych odrzucen; bez zmian bramek i sygnalow.'},
        pretty=True)
    # V6.177: diagnostyka w calosci po kalkulacjach, bez ingerencji w selekcje.
    qc_zmiany_v177 = raport_qc_zmiany_v177(current_rows, previous, qc_snapshot_status)
    qc_zmiany_cols_v177 = ('para','zmiana','odrzucone_przed','odrzucone_teraz',
                          'blokada_przed','blokada_teraz')
    write_csv('historia_qc_zmiany_v177.csv', qc_zmiany_v177,
              qc_zmiany_cols_v177, cols=qc_zmiany_cols_v177)
    qc_udzial_v177 = raport_qc_udzial_v177(historia_qc_przyczyny_v176,
                                          summary, live, top)
    qc_udzial_cols_v177 = cols_v176 + ('w_summary','w_live_setups','w_top_setups',
                                       'wpływ_na_backtest','uwaga')
    write_csv('historia_qc_udzial_v177.csv', qc_udzial_v177,
              qc_udzial_cols_v177, cols=qc_udzial_cols_v177)
    save_json('historia_qc_audyt_v177.json', {
        'wersja':VERSION,'status_porownania':qc_snapshot_status,
        'zmiany':qc_zmiany_v177,'udzial':qc_udzial_v177,
        'ograniczenie':'Bez sciezki zdarzen OHLCV nie mozna potwierdzic wplywu QC na historyczny wynik R.'},pretty=True)
    # V6.178: dwa raporty czysto diagnostyczne, po selekcji setupow.
    qc_skrot_v178=raport_qc_skrot_v178(qc_zmiany_v177)
    cols_skrot_v178=qc_zmiany_cols_v177
    write_csv('historia_qc_skrot_v178.csv',qc_skrot_v178,cols_skrot_v178,cols=cols_skrot_v178)
    qc_slad_v178=raport_qc_wejscia_v178(historia_qc_przyczyny_v176,
                                           historia_kontrola,summary,live,top)
    cols_slad_v178=('symbol','interwal','dowod_wejscia','swiece_dostepne','swiece_odrzucone',
      'blokuje_analize','w_summary','w_live_setups','w_top_setups',
      'czy_interwal_faktycznie_uzyty','wplyw_na_wynik_r')
    write_csv('historia_qc_slad_wejsc_v178.csv',qc_slad_v178,cols_slad_v178,cols=cols_slad_v178)
    save_json('historia_qc_slad_wejsc_v178.json',{'wersja':VERSION,
       'metoda':'laczenie kontroli dostepnych wejsc OHLCV z populacjami raportow',
       'ograniczenie':'Nie jest to instrumentacja wywolan strategii ani dowod uzycia konkretnej swiecy; nie przypisuje zmian wyniku R.',
       'skrot_zmian':qc_skrot_v178,'rekordy':qc_slad_v178},pretty=True)
    if drive_ok and not replay and qc_snapshot_status not in ('NIEZGODNY_FORMAT',) and not qc_snapshot_status.startswith('BLAD_ODCZYTU'):
        try:
            payload = {'schema': 1, 'saved_utc': utc_now(), 'rows': current_rows}
            zapisz_atomowo(qc_snapshot_file,
                lambda tmp: Path(tmp).write_text(json.dumps(payload, ensure_ascii=False, allow_nan=False), encoding='utf-8'),
                kopia=True)
        except (OSError, TypeError, ValueError) as exc:
            qc_snapshot_status = 'BLAD_ZAPISU: ' + str(exc)[:120]
    print(f'QC zmian: {len(qc_delta)} par zmienionych, {len(qc_repeat)} z odrzuconymi swiecami; '
          f'stan archiwum QC: {qc_snapshot_status}')
    print(f"Historia: {len(historia_problemy)} problematycznych par symbol/interwal; "
          f"{sum(bool(r['blokuje_analize']) for r in historia_problemy)} wpisow z lokalna blokada interwalu; "
          f"{sum(r['status'] == 'ODRZUCONE_SWIECE' and not r['blokuje_analize'] for r in historia_problemy)} "
          "ostrzezen o odrzuconych swiecach bez lokalnej blokady")
    save_json('qc.json', {k: v for k, v in qc.items() if k not in qc_info}, pretty=True)
    save_json('qc_info.json', qc_info, pretty=True)

    bf = market_features(btc_A, float(btc['arr']['5m'][-1, C]), now_ms)[0] if btc_A and enough(btc_A) else None
    (OUT/'identyfikacja_skryptu_v144.json').write_text(json.dumps(runtime_identity_v144(),ensure_ascii=False,indent=2),encoding='utf-8')
    (OUT/'identyfikacja_skryptu_v145.json').write_text(json.dumps(runtime_identity_v145(),ensure_ascii=False,indent=2),encoding='utf-8')
    (OUT/'manifest_sha256_v146.json').write_text(json.dumps(runtime_manifest_v146(),ensure_ascii=False,indent=2),encoding='utf-8')
    (OUT/'manifest_sha256_v147.json').write_text(json.dumps(runtime_manifest_v147(),ensure_ascii=False,indent=2),encoding='utf-8')
    (OUT/'manifest_sha256_v148.json').write_text(json.dumps(runtime_manifest_v148(),ensure_ascii=False,indent=2),encoding='utf-8')
    (OUT/'manifest_sha256_v149.json').write_text(json.dumps(runtime_manifest_v149(),ensure_ascii=False,indent=2),encoding='utf-8')
    (OUT/'manifest_sha256_v172.json').write_text(json.dumps(runtime_manifest_v172(),ensure_ascii=False,indent=2),encoding='utf-8')
    (OUT/'manifest_sha256_v173.json').write_text(json.dumps(runtime_manifest_v173(),ensure_ascii=False,indent=2),encoding='utf-8')
    (OUT/'manifest_sha256_v175.json').write_text(json.dumps(runtime_manifest_v175(),ensure_ascii=False,indent=2),encoding='utf-8')
    (OUT/'manifest_sha256_v177.json').write_text(json.dumps(runtime_manifest_v175(),ensure_ascii=False,indent=2),encoding='utf-8')
    (OUT/'manifest_sha256_v178.json').write_text(json.dumps(runtime_manifest_v175(),ensure_ascii=False,indent=2),encoding='utf-8')
    (OUT/'manifest_sha256_v179.json').write_text(json.dumps(runtime_manifest_v175(),ensure_ascii=False,indent=2),encoding='utf-8')
    (OUT/'manifest_sha256_v180.json').write_text(json.dumps(runtime_manifest_v175(),ensure_ascii=False,indent=2),encoding='utf-8')
    (OUT/'manifest_sha256_v181.json').write_text(json.dumps(runtime_manifest_v175(),ensure_ascii=False,indent=2),encoding='utf-8')
    (OUT/'manifest_sha256_v182.json').write_text(json.dumps(runtime_manifest_v175(),ensure_ascii=False,indent=2),encoding='utf-8')
    (OUT/'manifest_sha256_v183.json').write_text(json.dumps(runtime_manifest_v175(),ensure_ascii=False,indent=2),encoding='utf-8')
    _v182_after_candles = _v182_candle_snapshot(DRIVE_DIR) if drive_ok and not replay else {'status':'BRAK_ARCHIWUM'}
    _v182_changes = _v182_candle_diff(_v182_before_candles, _v182_after_candles)
    write_csv('historia_ohlcv_zmiany_v182.csv',_v182_changes, ['symbol','interwal','status','powod','nowe_na_koncu','uzupelnione_luki','korekty_istniejacych','usuniete','przed','po','uwaga'])
    save_json('historia_ohlcv_zmiany_v182.json', {'wersja':VERSION, 'status_przed':_v182_before_candles.get('status'), 'status_po':_v182_after_candles.get('status'), 'bledy_przed':_v182_before_candles.get('errors',{}), 'bledy_po':_v182_after_candles.get('errors',{}), 'zmiany':_v182_changes, 'ograniczenie':'Porownanie archiwow Drive przed/po, nie buforow wykorzystanych as-of'}, pretty=True)
    audyt_v179 = _v179_audit_run(OUT, DRIVE_DIR, drive_ok, replay, _fetch_stats, _history_repairs, funding_reasons, _v181_before_archive)
    meta = {'version': VERSION, 'manifest_sha256_v146':'manifest_sha256_v146.json', 'manifest_sha256_v147':'manifest_sha256_v147.json', 'manifest_sha256_v148':'manifest_sha256_v148.json', 'manifest_sha256_v149':'manifest_sha256_v149.json', 'manifest_sha256_v172':'manifest_sha256_v172.json', 'manifest_sha256_v173':'manifest_sha256_v173.json', 'manifest_sha256_v175':'manifest_sha256_v175.json', 'manifest_sha256_v177':'manifest_sha256_v177.json', 'manifest_sha256_v178':'manifest_sha256_v178.json', 'manifest_sha256_v179':'manifest_sha256_v179.json', 'manifest_sha256_v180':'manifest_sha256_v180.json', 'manifest_sha256_v181':'manifest_sha256_v181.json', 'manifest_sha256_v182':'manifest_sha256_v182.json', 'manifest_sha256_v183':'manifest_sha256_v183.json', 'historia_ohlcv_zmiany_v182':'historia_ohlcv_zmiany_v182.json', 'audyt_przebiegow_v179':audyt_v179, 'historia_qc_v178':{'skrot':len(qc_skrot_v178),'slad':len(qc_slad_v178),'dowod_wykonania_swiecy':False}, 'historia_qc_v177':{'zmiany':len(qc_zmiany_v177),'udzial':len(qc_udzial_v177),'status':qc_snapshot_status}, 'runtime_identity':runtime_identity_v145(), 'historical_slippage':{'model':HISTORICAL_SLIP_MODEL,'fallback_bps':float(HISTORICAL_SLIP_BPS),'scenario_bps':[10,20,30],'scenario_type':'linear_approximation_not_replay'}, 'finished_utc': utc_now(), 'server_time': iso(now_ms), 'seconds': round(time.time() - t_start, 1),
            'replay': REPLAY_ZIP or None, 'czasy_s': dict(_czasy), 'zapytania_api': n_api_pobieranie,
            'api_endpointy': {k: {**{a: int(b) for a, b in v.items() if a not in ('seconds', 'rate_wait_s')},
                                   'czas_http_s': round(v.get('seconds', 0), 3),
                                   'czas_limitera_s': round(v.get('rate_wait_s', 0), 3)}
                              for k, v in sorted(_api_by_endpoint.items())},
            'io_wyniki': dict(_io_outcomes), 'workers': WORKERS, 'max_req_per_sec': MAX_REQ_PER_SEC,
            'pobieranie_tryby': dict(_fetch_stats), 'historia_qc_przyczyny_v176_liczba': len(historia_qc_przyczyny_v176), 'historia_problemy_liczba': len(historia_problemy), 'historia_zera_zrodla': zero_summary, 'historia_zera_zdarzenia': zero_events_summary, 'historia_us30_weryfikacja': zero_verify_summary, 'historia_qc_porownanie': {'status': qc_snapshot_status, 'zmienione': len(qc_delta), 'odrzucone': len(qc_repeat), 'powtarzalne': sum(r['kolejne_uruchomienia_z_odrzuceniami'] > 1 for r in qc_repeat)}, 'historia_naprawy': {f'{sym}|{tf}': info for (sym, tf), info in _history_repairs.items() if info.get('powod') != 'archiwum_aktualne'}, 'funding_cache': dict(_funding_io),
            'bt_cache': {**_bt_cache_io, 'trafienia': int(_bt_stat.get('trafienia', 0)),
                         'liczone': int(_bt_stat.get('liczone', 0))}, 'symbols': len(coins), 'symbols_krypto': sum(1 for r_ in summary if r_.get('asset_type') == 'COIN'), 'symbols_tradfi': sum(1 for r_ in summary if r_.get('asset_type') != 'COIN'), 'setups_all': len(live), 'setups_ok': len(good),
            'top': [{k: r.get(k) for k in TOP_FIRST} for r in top],
            'tryb_ochronny': ochrona, 'hipoteza_blad': _HIP_BLAD.get('blad'), 'archiwum_blad': _ARCH_BLAD.get('blad'), 'warunki_globalne': warunki_glob, 'dzien': dzien, 'squeeze': squeeze, 'gate_tlum': gate_mkt,
            'badanie_rynku': {k: v for k, v in (study or {}).items() if k != 'flagi'}, 'alarm_btc': shock, 'ekspozycja': ekspozycja,
            'pozycje': pos_rows, 'btc_bias_5m': btc_bias, 'us_session_open': bool(us_open),
            'ranking': [{k: r.get(k) for k in RANK_FIRST} for r in rank_rows],
            'paging_5m': _paging['supported'], 'drive': drive_ok,
            'zegar': {'offset_ms': _clock.get('offset_ms'), 'rtt_ms': _clock.get('rtt_ms')},
            'jakosc_testu': [x for x in jak if x['grupa'] == 'wszystko'], 'proba': proba_meta, 'zakresy_testow': zakresy,
            'niezgodnosci_statusu': niezgodne, 'bramka_tryb': BRAMKA_TRYB, 'rozbieznosci': rozb,
            'odrzucone': {'n': int(len(odrz)), 'powody': odrz['powod_grupa'].value_counts().to_dict() if len(odrz) else {}},
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
    write_csv('api_endpoints.csv',
              [{'endpoint': k, 'zapytania': int(v['count']), 'czas_http_s': round(v['seconds'], 3),
                'sredni_http_ms': round(1000*v['seconds']/v['count'], 1) if v['count'] else 0}
               for k, v in sorted(_api_by_endpoint.items(), key=lambda kv: -kv[1]['seconds'])],
              ('endpoint', 'zapytania', 'czas_http_s', 'sredni_http_ms'))
    save_json('api_pobieranie_przyczyny_v170.json', {
        'version': VERSION,
        'endpointy': {str(k): {'count':int(v.get('count',0)),
                               'seconds':round(float(v.get('seconds',0)),3)}
                     for k,v in sorted(_api_by_endpoint.items())},
        'tryby_pobierania': dict(sorted(_fetch_stats.items())),
        'funding_cache': {'trafienia':int(n_fz) if 'n_fz' in locals() else None,
                          'liczba_instrumentow':len(coins)},
        'ograniczenie': 'Liczniki z jednego przebiegu. Przyczyny pobierania sa globalne, nie sa 1:1 przypisane do endpointow. Roznica miedzy przebiegami wymaga porownania dwoch raportow.'
    },pretty=True)
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
    print_report(good, live, split_stocks(bt_df)[1] if len(bt_df) else bt_df, arch, gate, {**meta, '_hist_ok': hist_df[hist_df.filtr.fillna('') == ''] if len(hist_df) else None}, top, htab)
    stamp = datetime.fromtimestamp(now_ms / 1000, tz=timezone.utc).strftime('%Y%m%d_%H%M%S')
    zpath = OUT_ROOT / f"weex_v6_{stamp}{'_BEZ_DYSKU' if bez_dysku else ''}.zip"   # V6.42
    with etap('zip'):
        audit_export_v171 = csv_finalny_audyt_v171(OUT)
        print('V6.171: kontrola CSV:', len(audit_export_v171), 'plikow; scalono:', sum(r['status']=='IDENTYCZNE_SCALONE' for r in audit_export_v171))
        with zipfile.ZipFile(zpath, 'w', zipfile.ZIP_DEFLATED, compresslevel=1) as z:   # V6.8: szybka kompresja
            for p in OUT.rglob('*'):
                if p.is_file():
                    z.write(p, p.relative_to(OUT_ROOT))
    print(f'\nZapisano {zpath} ({zpath.stat().st_size / 1e6:.1f} MB) w {time.time() - t_start:.0f}s')
    print('Czasy etapow: ' + ', '.join(f'{k} {v:.0f}s' for k, v in _czasy.items()))
    if bez_dysku:
        baner_brak_dysku()
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
        if OCENA_RR_PKT and (r.get('rr_tp1_net') or 0) >= 2.5:
            pkt += OCENA_RR_PKT; why.append(f"R:R {r['rr_tp1_net']} ({OCENA_RR_PKT:+d})")
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
        if r.get('setup_type') in OCENA_STALA:          # V6.53
            grade = OCENA_STALA[r['setup_type']]
            why.append(f'ocena stala {grade} dla {r["setup_type"]} (punkty A-D nie przewiduja wynikow)')
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


def pozycje_z_ryzykiem(positions, side):
    """V6.46: ile otwartych pozycji w kierunku ma jeszcze ryzyko (brak SL albo SL gorszy niz wejscie)."""
    n = 0
    for p in positions or []:
        if str(p.get('side', '')).upper() != side:
            continue
        e, sl = _num(p.get('entry')), _num(p.get('sl'))
        if e is None or sl is None:
            n += 1
        elif (side == 'LONG' and sl < e) or (side == 'SHORT' and sl > e):
            n += 1
    return n


def limit_ekspozycji(live):
    """V6.46: setup 'DO GRY' poza wyborem w limicie ryzyka na kierunek -> nie do handlu (OBSERWACJA). Zwraca liczbe."""
    n = 0
    for r in live:
        if r.get('do_handlu') and r.get('status_bramki') == 'DO GRY' and not r.get('w_limicie_ryzyka'):
            r['do_handlu'] = False
            w = str(r.get('warunki') or '')
            r['warunki'] = (w + '; ' if w and w != 'OK' else '') + f'limit ekspozycji ({MAX_RYZYKO_KIERUNEK_PCT}% na kierunek)'
            r['status_setupu'] = status_setupu(r)
            w_ = [x.strip() for x in r['warunki'].split(';') if x.strip() and x.strip() != 'OK']
            bez = ('tryb ochronny', 'alarm BTC', 'dzienny limit', 'nie tradable')
            r['blokady_bezpieczenstwa'] = '; '.join(x for x in w_ if x.startswith(bez))
            r['blokady_kwalifikacji'] = '; '.join(x for x in w_ if not x.startswith(bez))
            n += 1
    return n


def test_ekspozycji():
    """V6.46: wybor w limicie + blokada pozostalych DO GRY + liczenie pozycji z ryzykiem (test na limicie 3%)."""
    global MAX_RYZYKO_KIERUNEK_PCT
    stary = MAX_RYZYKO_KIERUNEK_PCT
    MAX_RYZYKO_KIERUNEK_PCT = 3.0
    try:
        return _test_ekspozycji()
    finally:
        MAX_RYZYKO_KIERUNEK_PCT = stary


def _test_ekspozycji():
    def mk(sym, ocena='B', side='LONG', bramka='DO GRY', et='LIMIT'):
        return {'symbol': sym, 'side': side, 'ocena': ocena, 'ocena_pkt': 1, 'entry_type': et, 'rr_tp1_net': 2.0,
                'do_handlu': True, 'status_bramki': bramka, 'warunki': 'OK', 'cost_R': 0.05}
    live = [mk('A1', 'A'), mk('B1'), mk('B2'), mk('B3'), mk('S1', side='SHORT')]
    ryz = 1.5
    wybor_ryzyka(live, 'LONG', ryz, 0)
    wybor_ryzyka(live, 'SHORT', ryz, 0)
    n = limit_ekspozycji(live)
    ok = {r['symbol'] for r in live if r['do_handlu']}
    assert ok == {'A1', 'B1', 'S1'} and n == 2, f'test ekspozycji 1: {ok} {n}'
    assert all(r['status_setupu'] == 'OBSERWACJA' for r in live if not r['do_handlu']), 'test ekspozycji 2: status'
    pos = [{'side': 'LONG', 'entry': 1.0, 'sl': 0.9}, {'side': 'LONG', 'entry': 1.0, 'sl': 1.0},
           {'side': 'LONG', 'entry': 1.0, 'sl': 1.05}, {'side': 'LONG', 'entry': 1.0}, {'side': 'SHORT', 'entry': 1.0, 'sl': 1.1}]
    assert pozycje_z_ryzykiem(pos, 'LONG') == 2 and pozycje_z_ryzykiem(pos, 'SHORT') == 1, 'test ekspozycji 3: pozycje'
    live = [mk('A1', 'A'), mk('B1')]
    wybor_ryzyka(live, 'LONG', ryz, 2)                 # limit zajety przez 2 pozycje z ryzykiem
    assert limit_ekspozycji(live) == 2, 'test ekspozycji 4: limit zajety'
    live = [mk('P1', bramka='PROBA'), mk('B1')]
    wybor_ryzyka(live, 'LONG', ryz, 2)
    limit_ekspozycji(live)
    assert live[0]['do_handlu'] and not live[1]['do_handlu'], 'test ekspozycji 5: PROBA ma wlasny limit'
    pos = [{'side': 'SHORT', 'entry': 1.0, 'sl': 1.05}, {'side': 'SHORT', 'entry': 1.0, 'sl': 1.02},   # SL przesuniety czesciowo
           {'side': 'SHORT', 'entry': 1.0, 'sl': 1.0}, {'side': 'LONG', 'entry': 1.0, 'sl': 0.95}]
    assert pozycje_z_ryzykiem(pos, 'SHORT') == 2 and pozycje_z_ryzykiem(pos, 'LONG') == 1, 'test ekspozycji 6: czesciowy SL / obie strony'
    return 'OK (6 przypadkow)'


def test_ekspozycji_konfig():
    """V6.49: limit na rzeczywistej konfiguracji (MAX_RYZYKO_KIERUNEK_PCT / RYZYKO_NA_TRANSAKCJE_PCT)."""
    def mk(sym, side='LONG'):
        return {'symbol': sym, 'side': side, 'ocena': 'B', 'ocena_pkt': 1, 'entry_type': 'LIMIT', 'rr_tp1_net': 2.0,
                'do_handlu': True, 'status_bramki': 'DO GRY', 'warunki': 'OK', 'cost_R': 0.05}
    ryz = RYZYKO_NA_TRANSAKCJE_PCT
    miejsca = int((MAX_RYZYKO_KIERUNEK_PCT + 1e-9) // ryz)
    live = [mk(f'L{i}') for i in range(miejsca + 2)] + [mk(f'S{i}', 'SHORT') for i in range(miejsca + 1)]
    wybor_ryzyka(live, 'LONG', ryz, 1)                 # 1 pozycja LONG z ryzykiem
    wybor_ryzyka(live, 'SHORT', ryz, 0)
    limit_ekspozycji(live)
    nl = sum(1 for r in live if r['side'] == 'LONG' and r['do_handlu'])
    ns = sum(1 for r in live if r['side'] == 'SHORT' and r['do_handlu'])
    assert nl == miejsca - 1 and ns == miejsca, f'test konfiguracji: LONG {nl}, SHORT {ns}, miejsca {miejsca}'
    return f'OK ({MAX_RYZYKO_KIERUNEK_PCT}% / {ryz}% = {miejsca} miejsca na kierunek)'


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
    dodatnia i t >= TOP_MIN_T; najwyzej TOP_MAX_PER_SIDE w jednym kierunku i jeden setup na kontrakt (caly TOP).
    Kolejnosc: status bramki, ocena (score_R), R:R netto, nizszy koszt."""
    order = {'DO GRY': 0, 'PROBA': 1, 'OBSERWACJA': 2}
    cand = [r for r in good if r['score_R'] > 0 and (r.get('hist_t_edge') or 0) >= TOP_MIN_T and r.get('ocena') != 'D'
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
           'gate_top_lsr_konta': rnd(last.top_lsr_account, 3),
           'gate_wiek_h': rnd((ts - int(last.time_ms) - HOUR_MS) / HOUR_MS, 2)}   # V6.63: od zamkniecia ostatniej godziny
    w = pd.to_numeric(x.lsr_account.iloc[-48:], errors='coerce').dropna()
    if len(w) >= 24 and w.std() > 0:
        out['gate_lsr_z'] = rnd((w.iloc[-1] - w.mean()) / w.std(), 2)
    l4 = x.iloc[-4:]
    ll, sl_ = pd.to_numeric(l4.long_liq_usd, errors='coerce').sum(), pd.to_numeric(l4.short_liq_usd, errors='coerce').sum()
    out['gate_likw_long_4h_usd'], out['gate_likw_short_4h_usd'] = rnd(ll, 0), rnd(sl_, 0)
    out['gate_likw_long_udzial'] = rnd(ll / (ll + sl_), 2) if ll + sl_ > 0 else None
    oi = pd.to_numeric(x.open_interest_usd, errors='coerce')
    if len(oi) and pd.notna(oi.iloc[-1]):
        out['gate_oi_usd'] = rnd(oi.iloc[-1], 0)            # V6.16: do porownania z OI WEEX
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
                dopisz_log(Path(DRIVE_DIR) / GATE_LOG, last)      # V6.63
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
    cols = ['typ_wersja', 'side', 'transakcje', 'sredni_R', 'sredni_edge_R', 't_edge', 'skutecznosc_pct', 'zdjecia', 'odblokowany',
            'transakcje_sesja_USA', 'sredni_R_sesja_USA', 'transakcje_poza_sesja', 'sredni_R_poza_sesja']   # V6.88
    if d is None or not len(d) or 'edge_R' not in d.columns:
        return pd.DataFrame(columns=cols)
    d = d.copy()
    f = d.filtr.fillna('').str.replace('tradfi_bez_testu', '', regex=False).str.strip(';')
    d = d[(f == '') & d.status.isin(FINAL)]
    d['r'] = pd.to_numeric(d['r'], errors='coerce')
    d['edge_R'] = pd.to_numeric(d['edge_R'], errors='coerce')
    t = pd.to_datetime(d['snapshot_utc'], utc=True, errors='coerce') if 'snapshot_utc' in d.columns else pd.Series(pd.NaT, index=d.index)
    d['_usa'] = [bool(pd.notna(x) and us_session_open(int(x.value // 10**6))) for x in t]
    rows = []
    for (tv, side), g in d.groupby(['typ_wersja', 'side']):
        st = stats(g)
        gu, gp = g[g['_usa']], g[~g['_usa']]
        ok = (st.get('transakcje', 0) >= TRADFI_MIN_N and (st.get('sredni_R') or -9) > 0
              and (st.get('sredni_edge_R') or -9) > 0 and (st.get('t_edge') or -9) >= TRADFI_MIN_T)
        rows.append({'typ_wersja': tv, 'side': side, 'transakcje': st.get('transakcje', 0), 'sredni_R': st.get('sredni_R'),
                     'sredni_edge_R': st.get('sredni_edge_R'), 't_edge': st.get('t_edge'),
                     'skutecznosc_pct': st.get('skutecznosc_pct'), 'zdjecia': st.get('zdjecia'), 'odblokowany': bool(ok),
                     'transakcje_sesja_USA': len(gu), 'sredni_R_sesja_USA': rnd(gu['r'].mean(), 3) if len(gu) else None,
                     'transakcje_poza_sesja': len(gp), 'sredni_R_poza_sesja': rnd(gp['r'].mean(), 3) if len(gp) else None})
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
        r['hist_t_edge'], r['hist_zrodlo'] = (x.get('t_edge') if x else None), 'test akcji (24h od V6.88; wczesniej sesja USA)'
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
            dopisz_log(Path(DRIVE_DIR) / SQUEEZE_LOG, df)         # V6.63
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


PROBA_COLS = ['event_id', 'snapshot_utc', 'symbol', 'side', 'setup_type', 'typ_wersja', 'entry_type', 'ocena', 'entry', 'sl',
              'tp1', 'tp_1R', 'wait_h', 'slip_bps', 'ryzyko_mnoznik', 'status', 'r', 'fill_utc', 'exit_utc', 'niejednoznaczne']


def proba_rekord_blad(x):
    """V6.25: powod, dla ktorego rekord dziennika PROBA nie nadaje sie do rozliczenia ('' = OK)."""
    if not isinstance(x.get('symbol'), str) or not x.get('symbol'):
        return 'brak symbolu'
    if x.get('side') not in ('LONG', 'SHORT'):
        return f"zla strona {x.get('side')!r}"
    try:
        t = pd.Timestamp(str(x.get('snapshot_utc'))[:16], tz='UTC')
        if pd.isna(t) or not (pd.Timestamp('2025-01-01', tz='UTC') <= t <= pd.Timestamp.now(tz='UTC') + pd.Timedelta(minutes=10)):
            return f"zla data {x.get('snapshot_utc')!r}"
    except Exception:
        return f"zla data {x.get('snapshot_utc')!r}"
    e, sl, tp = _num(x.get('entry')), _num(x.get('sl')), _num(x.get('tp1'))
    if None in (e, sl, tp) or min(e, sl, tp) <= 0:
        return 'brak/zle ceny'
    if not ((sl < e < tp) if x['side'] == 'LONG' else (tp < e < sl)):
        return 'zla kolejnosc SL/entry/TP1'
    w = _num(x.get('wait_h'))
    if w is None or w < 0 or w > 72:
        return f"zle wait_h {x.get('wait_h')!r}"
    if x.get('entry_type') not in ('LIMIT', 'MARKET'):
        return f"zly typ wejscia {x.get('entry_type')!r}"
    return ''


def test_proba_rekordy():
    """V6.25: test walidacji rekordow dziennika PROBA."""
    ok = {'symbol': 'XUSDT', 'side': 'LONG', 'snapshot_utc': '2026-10-19T08:00', 'entry': 1.0, 'sl': 0.95, 'tp1': 1.1,
          'wait_h': 4, 'entry_type': 'LIMIT'}
    zle = [{'side': 'X'}, {'snapshot_utc': 'abc'}, {'snapshot_utc': '1999-01-01T00:00'}, {'snapshot_utc': '2099-01-01T00:00'},
           {'entry': float('nan')}, {'sl': float('inf')}, {'tp1': -1}, {'sl': 1.05}, {'side': 'SHORT'},
           {'wait_h': None}, {'entry_type': 'STOP'}, {'symbol': None}]
    assert proba_rekord_blad(ok) == '', proba_rekord_blad(ok)
    bad = [z for z in zle if not proba_rekord_blad({**ok, **z})]
    assert not bad, f'TEST DZIENNIKA PROBA NIEUDANY: przepuszczone {bad}'
    assert proba_rekord_blad({**ok, 'side': 'SHORT', 'sl': 1.05, 'tp1': 0.9}) == ''
    return f'OK ({len(zle) + 2} przypadkow)'


def _proba_rozlicz(log, PP):
    """V6.25/V6.39: rozlicza otwarte rekordy dziennika prob (w miejscu)."""
    if not len(log):
        return
    for k in ('status', 'fill_utc', 'exit_utc', 'niejednoznaczne'):   # V6.67: kolumny tekstowe jako object - pusta
        if k in log.columns:                                         # kolumna wczytana jako float64 nie przyjmie
            log[k] = log[k].astype(object)                           # tekstu (pandas 3: TypeError, 2.x: ostrzezenie)
    open_ = ~log.status.isin(list(FINAL) + ['niewypelniony', 'anulowany', 'luka_danych', 'blad_rekordu'])
    for i in log.index[open_]:
        x = log.loc[i].to_dict()
        why_bad = proba_rekord_blad(x)                 # V6.25
        if why_bad:
            log.at[i, 'status'] = 'blad_rekordu'
            log.at[i, 'niejednoznaczne'] = why_bad
            continue
        P = PP.get(x['symbol'])
        if not P or '5m' not in P['arr']:
            continue
        ts = int(pd.Timestamp(str(x['snapshot_utc'])[:16], tz='UTC').timestamp() * 1000)
        a5 = P['arr']['5m']
        if not len(a5) or a5[0, T] > ts:
            continue
        row = {k: x.get(k) for k in ('side', 'entry', 'sl', 'tp1', 'entry_type', 'setup_type', 'slip_bps')}
        row['wait_h'] = float(x.get('wait_h') or 0)
        if row.get('slip_bps') is None or (isinstance(row['slip_bps'], float) and math.isnan(row['slip_bps'])):
            row['slip_bps'] = None
        for k in ('entry', 'sl', 'tp1'):
            row[k] = float(row[k])
        try:
            res = simulate(a5, ts, row, P['fees'])
        except Exception as exc:                       # V6.25: jeden zly rekord nie przerywa reszty
            log.at[i, 'status'] = 'blad_rekordu'
            log.at[i, 'niejednoznaczne'] = f'simulate: {str(exc)[:80]}'
            continue
        for k in ('status', 'r', 'fill_utc', 'exit_utc', 'niejednoznaczne'):
            log.at[i, k] = res.get(k)


PROBA_AKTYWNE = ('oczekuje', 'otwarty')        # V6.39: statusy dziennika liczone do limitu prob
PROBA_AKTYWNA_MAX_H = 48        # V6.41: limit wieku tylko dla 'oczekuje'; 'otwarty' liczy sie az do rozliczenia


def _proba_blokuj(rows, powod):
    """V6.49: proby nie do handlu z powodem (blad dziennika)."""
    for r in rows:
        r['do_handlu'] = False
        r['status_bramki'] = 'PROBA'
        w = str(r.get('warunki') or '')
        r['warunki'] = (w + '; ' if w and w != 'OK' else '') + powod


def test_proba_dziennik():
    """V6.49: blad odczytu -> brak zapisu i brak prob; blad zapisu -> nowe proby nie do handlu."""
    import tempfile, contextlib
    global DRIVE_DIR, OUT
    stary, stary_out = DRIVE_DIR, OUT
    def mk(sym):
        return {'symbol': sym, 'side': 'LONG', 'do_handlu': True, 'status_bramki': 'OBSERWACJA', 'setup_type': 'PULLBACK',
                'typ_wersja': 'PULLBACK.v4', 'entry_type': 'LIMIT', 'entry': 1.0, 'sl': 0.9, 'tp1': 1.2, 'ocena': 'B',
                'snapshot_utc': '2026-10-22T10:00', 'warunki': 'OK'}
    try:
        with tempfile.TemporaryDirectory() as td:
            DRIVE_DIR = td
            OUT = Path(td) / 'out'                     # wyniki testu poza katalogiem raportu
            OUT.mkdir()
            fp = Path(td) / PROBA_LOG
            fp.mkdir()                                 # 'plik' dziennika nieczytelny (katalog)
            live = [mk('A'), mk('B')]
            with contextlib.redirect_stdout(io.StringIO()):   # komunikaty bledow testowych nie do raportu
                m = proba_select_and_log(live, {}, True, parse_utc('2026-10-22T10:00'))
            assert m.get('blad_dziennika') == 'odczyt', f'test dziennika 1: {m}'
            assert not any(r['do_handlu'] for r in live), 'test dziennika 2: proby mimo bledu odczytu'
            assert fp.is_dir() and not any(fp.iterdir()), 'test dziennika 3: dziennik ruszony po bledzie odczytu'
            plik = Path(td) / 'plik'
            plik.write_text('x')
            DRIVE_DIR = str(plik)                      # katalog dziennika jest plikiem -> blad zapisu
            live = [mk('A'), mk('B')]
            with contextlib.redirect_stdout(io.StringIO()):   # komunikaty bledow testowych nie do raportu
                m = proba_select_and_log(live, {}, True, parse_utc('2026-10-22T10:00'))
            assert m.get('blad_dziennika') == 'zapis' and m.get('nowe') == 0, f'test dziennika 4: {m}'
            assert not any(r['do_handlu'] for r in live), 'test dziennika 5: nowe proby mimo bledu zapisu'
            assert all('blad zapisu' in r['warunki'] for r in live), 'test dziennika 6: powod'
    finally:
        DRIVE_DIR, OUT = stary, stary_out
    return 'OK (6 przypadkow)'


def _proba_opis(x):
    """V6.67: krotki opis proby z dziennika do kolumny 'warunki'."""
    try:
        od = czas_pl(int(pd.Timestamp(str(x.get('snapshot_utc'))[:16], tz='UTC').timestamp() * 1000)) + ' PL'
    except Exception:
        od = str(x.get('snapshot_utc'))
    return (f"{x.get('typ_wersja')} {x.get('entry_type')} wejscie {x.get('entry')}, SL {x.get('sl')}, "
            f"od {od}, status {x.get('status')}")


def proba_wybierz(cand, aktywne, aktywne_info=None):
    """V6.39: wybor prob. aktywne = {(symbol, side)} z dziennika (otwarte + oczekujace). Zwraca liste NOWYCH prob.
    V6.67: aktywne_info = {(symbol, side): [rekordy dziennika]} (z event_id, entry, sl, ...). Kontynuacja tylko dla
    TEGO SAMEGO zlecenia: ten sam event_id, LIMIT i to samo wejscie (to samo zlecenie czeka dalej). Inny setup na
    coinie+kierunku z aktywna proba (inna okazja, MARKET po innej cenie) -> nie do handlu z opisem proby z dziennika
    (wczesniej: do_handlu bez limitu i bez wpisu do dziennika -> mozliwa druga pozycja na tym samym coinie).
    aktywne_info=None -> zachowanie sprzed V6.67 (tylko symbol + kierunek)."""
    wolne = max(0, PROBA_MAX_NA_RAZ - len(aktywne))
    nowe = []
    for r in cand:
        r['status_bramki'] = 'PROBA'
        klucz = (r.get('symbol'), r.get('side'))
        if klucz in aktywne:
            wpisy = (aktywne_info or {}).get(klucz) if aktywne_info is not None else None
            if wpisy is None:
                r['proba_kontynuacja'] = True           # (tryb bez aktywne_info - jak przed V6.67)
                r['proba_stan'] = 'KONTYNUACJA (bez szczegolow dziennika)'
                continue
            ten_sam = [x for x in wpisy if str(x.get('event_id')) == str(r.get('event_id'))
                       and r.get('entry_type') == 'LIMIT' and _num(x.get('entry')) is not None
                       and _num(r.get('entry')) is not None
                       and abs(float(x['entry']) - float(r['entry'])) <= 1e-9 * max(1.0, abs(float(r['entry'])))]
            if ten_sam:
                r['proba_kontynuacja'] = True           # to samo zlecenie LIMIT co w dzienniku - bez nowego miejsca
                statusy = {str(x.get('status')) for x in ten_sam}
                if 'otwarty' in statusy or 'oczekuje' not in statusy:   # V6.69/V6.70: najpierw 'otwarty' - zawsze blokada
                    otw = [x for x in ten_sam if str(x.get('status')) == 'otwarty'] or ten_sam
                    r['do_handlu'] = False
                    r['proba_stan'] = 'POZYCJA JUZ OTWARTA'
                    w = str(r.get('warunki') or '')
                    r['warunki'] = ((w + '; ' if w and w != 'OK' else '') + 'POZYCJA Z TEJ PROBY JUZ OTWARTA - nie otwieraj '
                                    f"ponownie ({_proba_opis(otw[0])})")
                else:
                    r['proba_stan'] = 'KONTYNUACJA ZLECENIA'
                continue
            r['proba_kontynuacja'] = False
            r['do_handlu'] = False
            r['proba_stan'] = 'ZABLOKOWANA: INNA PROBA NA COINIE'
            opis = '; '.join(_proba_opis(x) for x in wpisy[:2]) if wpisy else 'brak szczegolow'
            w = str(r.get('warunki') or '')
            r['warunki'] = (w + '; ' if w and w != 'OK' else '') + f'na tym coinie jest juz aktywna proba ({opis})'
            continue
        if any((x.get('symbol'), x.get('side')) == (r.get('symbol'), r.get('side')) for x in nowe):
            r['do_handlu'] = False                      # drugi setup na tym samym coinie i kierunku w jednym raporcie
            r['warunki'] = 'proba na tym coinie juz wybrana'
            r['proba_stan'] = 'ZABLOKOWANA: DRUGI SETUP W RAPORCIE'
            continue
        if len(nowe) < wolne:
            nowe.append(r)
            r['proba_stan'] = 'NOWA'
        else:
            r['do_handlu'] = False
            r['proba_stan'] = 'ZABLOKOWANA: LIMIT PROB'
            r['warunki'] = (f'limit proby ({PROBA_MAX_NA_RAZ} na raz, aktywne w dzienniku: {len(aktywne)})'
                            if aktywne else f'limit proby ({PROBA_MAX_NA_RAZ} na raz)')
    return nowe


def test_proba_limit():
    """V6.39: limit prob z uwzglednieniem dziennika."""
    def mk(sym, side='LONG'):
        return {'symbol': sym, 'side': side, 'do_handlu': True, 'status_bramki': 'OBSERWACJA'}
    przypadki = [
        ('pusty dziennik, 3 kandydaci', [mk('A'), mk('B'), mk('C')], set(), ['A', 'B'], {'C'}),
        ('2 aktywne w dzienniku, 2 nowi', [mk('A'), mk('B')], {('X', 'LONG'), ('Y', 'LONG')}, [], {'A', 'B'}),
        ('1 aktywna, 2 nowi', [mk('A'), mk('B')], {('X', 'LONG')}, ['A'], {'B'}),
        ('kontynuacja nie zajmuje miejsca', [mk('X'), mk('A'), mk('B')], {('X', 'LONG'), ('Y', 'LONG')}, [], {'A', 'B'}),
        ('kontynuacja + 1 wolne', [mk('X'), mk('A'), mk('B')], {('X', 'LONG')}, ['A'], {'B'}),
        ('ten sam coin 2x w raporcie', [mk('A'), mk('A'), mk('B')], set(), ['A', 'B'], set()),
    ]
    for nazwa, cand, akt, oczek_nowe, oczek_zablok in przypadki:
        nowe = proba_wybierz(cand, akt)
        got = [r['symbol'] for r in nowe]
        zablok = {r['symbol'] for r in cand if not r['do_handlu'] and r['warunki'].startswith('limit')}
        assert got == oczek_nowe, f'test limitu prob "{nazwa}": nowe {got} != {oczek_nowe}'
        assert zablok == oczek_zablok, f'test limitu prob "{nazwa}": zablokowane {zablok} != {oczek_zablok}'
        assert all(r['do_handlu'] for r in cand if r.get('proba_kontynuacja')), f'test limitu prob "{nazwa}": kontynuacja'
    # V6.67: kontynuacja tylko dla tego samego zlecenia (event_id + LIMIT + wejscie)
    def mk2(sym, ev, typ='LIMIT', entry=1.0):
        return {**mk(sym), 'event_id': ev, 'entry_type': typ, 'entry': entry, 'warunki': 'OK'}
    log_x = {'event_id': 'e1', 'entry': 1.0, 'sl': 0.9, 'typ_wersja': 'T.v1', 'entry_type': 'LIMIT',
             'snapshot_utc': '2026-10-24T13:41', 'status': 'oczekuje'}
    info = {('X', 'LONG'): [log_x]}
    log_o = {**log_x, 'event_id': 'e9', 'status': 'otwarty'}                       # V6.69: zlecenie juz wykonane
    log_d1 = {**log_x, 'event_id': 'e7', 'status': 'oczekuje'}                     # V6.70: niespojny dziennik -
    log_d2 = {**log_x, 'event_id': 'e7', 'status': 'otwarty'}                      # ten sam event 2x
    info = {('X', 'LONG'): [log_x, log_o, log_d1, log_d2]}
    p2 = [('ten sam LIMIT = kontynuacja', mk2('X', 'e1'), True, True),
          ('ten sam LIMIT juz wykonany', mk2('X', 'e9'), True, False),
          ('ten sam event: oczekuje + otwarty', mk2('X', 'e7'), True, False),
          ('inna okazja na tym coinie', mk2('X', 'e2'), False, False),
          ('ten sam event, MARKET po innej cenie', mk2('X', 'e1', 'MARKET', 0.8), False, False),
          ('ten sam event, LIMIT po innej cenie', mk2('X', 'e1', 'LIMIT', 0.95), False, False)]
    for nazwa, r, kont, dh in p2:
        nowe = proba_wybierz([r, mk2('A', 'ea')], {('X', 'LONG')}, info)
        assert bool(r.get('proba_kontynuacja')) == kont and r['do_handlu'] == dh, f'test limitu prob "{nazwa}": {r}'
        assert dh or 'juz aktywna proba' in r['warunki'] or 'JUZ OTWARTA' in r['warunki'], f'test limitu prob "{nazwa}": brak powodu'
        assert [x['symbol'] for x in nowe] == ['A'], f'test limitu prob "{nazwa}": nowe {nowe}'
        oczek_stan = ('KONTYNUACJA ZLECENIA' if dh else 'POZYCJA JUZ OTWARTA') if kont else 'ZABLOKOWANA: INNA PROBA NA COINIE'
        assert r.get('proba_stan') == oczek_stan, f'test limitu prob "{nazwa}": proba_stan {r.get("proba_stan")}'
    cand = [mk('A'), mk('A'), mk('B'), mk('C')]                                       # V6.70: stany bez dziennika
    proba_wybierz(cand, set())
    stany = [r.get('proba_stan') for r in cand]
    assert stany == ['NOWA', 'ZABLOKOWANA: DRUGI SETUP W RAPORCIE', 'NOWA', 'ZABLOKOWANA: LIMIT PROB'], f'stany: {stany}'
    return f'OK ({len(przypadki) + len(p2) + 1} przypadkow)'


def proba_aktywne_do_limitu(akt, now_ms):
    """Otwarte pozycje nie wygasaja od samego wieku; stare oczekujace nie blokuja bez konca."""
    if not len(akt):
        return akt
    granica = snap_iso(now_ms - PROBA_AKTYWNA_MAX_H * HOUR_MS)
    return akt[(akt.status == 'otwarty') |
               ((akt.status == 'oczekuje') & (akt.snapshot_utc.astype(str).str[:16] >= granica))]


def test_proba_wiek():
    """Regresja: pozycja otwarta po >48h zajmuje slot, stara oczekujaca nie."""
    now_ms = 1_800_000_000_000
    old = snap_iso(now_ms - 72 * HOUR_MS)
    recent = snap_iso(now_ms - 2 * HOUR_MS)
    frame = pd.DataFrame([
        {'status': 'otwarty', 'snapshot_utc': old, 'symbol': 'A'},
        {'status': 'oczekuje', 'snapshot_utc': old, 'symbol': 'B'},
        {'status': 'oczekuje', 'snapshot_utc': recent, 'symbol': 'C'},
    ])
    got = set(proba_aktywne_do_limitu(frame, now_ms).symbol)
    assert got == {'A', 'C'}, f'test wieku PROBA: {got}'
    return 'OK (3 rekordy)'


def proba_select_and_log(live, PP, drive_ok, now_ms):
    """V6.20/V6.39: etykieta PROBA, limit PROBA_MAX_NA_RAZ (z proba otwartymi i oczekujacymi z dziennika),
    dziennik proby rozliczany do przodu (simulate na nowych swiecach)."""
    if BRAMKA_TRYB != 'probny' or (not drive_ok and not REPLAY_ZIP):
        return {}
    cand = [r for r in live if r.get('do_handlu') and r.get('status_bramki') != 'DO GRY'
            and not r.get('tradfi_bez_blokad')]        # V6.92: TradFi poza PROBA
    cand.sort(key=lambda r: (-(r.get('score_R') or 0), r.get('ocena') or 'Z', -(r.get('rr_tp1_net') or 0)))
    path = Path(DRIVE_DIR) / PROBA_LOG
    log = pd.DataFrame(columns=PROBA_COLS)
    if drive_ok and path.exists():
        try:
            log = pd.read_csv(path)
        except Exception as exc:                       # V6.49: fail closed, NIE nadpisuj dziennika
            print(f'!!! Dziennik proby: blad odczytu ({str(exc)[:80]}) - proby wstrzymane, dziennik nie zapisany')
            _proba_blokuj(cand, 'dziennik PROBA: blad odczytu')
            return {'blad_dziennika': 'odczyt', 'kandydaci_teraz': len(cand), 'teraz_do_handlu': 0}
    if not drive_ok and cand:
        print('UWAGA: brak Google Drive - dziennik PROBA niedostepny: limit prob liczy tylko ten raport, '
              'a proby z tego uruchomienia NIE beda rozliczane w kolejnych uruchomieniach')
    _proba_rozlicz(log, PP)                            # V6.39: najpierw rozliczenie (wygasle zwalniaja miejsce)
    akt = log[log.status.isin(PROBA_AKTYWNE)] if len(log) else log
    akt = proba_aktywne_do_limitu(akt, now_ms)
    aktywne = {(str(x['symbol']), str(x['side'])) for _, x in akt.iterrows()} if len(akt) else set()
    aktywne_info = {}                                  # V6.67: szczegoly aktywnych prob (event_id, wejscie, SL)
    for _, x in (akt.iterrows() if len(akt) else []):
        aktywne_info.setdefault((str(x['symbol']), str(x['side'])), []).append(x.to_dict())
    if cand:                                           # V6.67: event_id kandydatow (jak w dzienniku)
        try:
            ev = add_event_ids(pd.DataFrame(cand))['event_id'].tolist()
            for r, e in zip(cand, ev):
                r['event_id'] = e
        except Exception as exc:
            print(f'Proba: blad event_id kandydatow ({str(exc)[:80]}) - setupy na coinach z aktywna proba zablokowane')
    if len(aktywne) > PROBA_MAX_NA_RAZ:                 # V6.43
        print(f'UWAGA: w dzienniku {len(aktywne)} aktywnych prob (limit {PROBA_MAX_NA_RAZ}): '
              f"{', '.join(sorted(a_[0] for a_ in aktywne))} - nowe proby wstrzymane do rozliczenia")
    new = proba_wybierz(cand, aktywne, aktywne_info)
    if new:
        nd = add_event_ids(pd.DataFrame(new))
        nd['status'] = 'oczekuje'
        # V6.173: nie konkatenowac pustego/all-NA dziennika (pandas FutureWarning).
        incoming = nd[[c for c in PROBA_COLS if c in nd.columns]].copy()
        if log.empty:
            log = incoming.reindex(columns=list(dict.fromkeys([*log.columns, *incoming.columns])))
        else:
            log = pd.concat([log, incoming], ignore_index=True)
        log = log.drop_duplicates('event_id', keep='first')
        _proba_rozlicz(log, PP)
    blad_zapisu = False
    if drive_ok and len(log):
        try:
            zapisz_atomowo(path, lambda t: log.to_csv(t, index=False))   # V6.59
        except Exception as exc:                       # V6.49: nowe proby bez zapisu = nie do handlu
            blad_zapisu = True
            print(f'!!! Dziennik proby: blad zapisu ({str(exc)[:80]}) - nowe proby wstrzymane')
            _proba_blokuj(new, 'dziennik PROBA: blad zapisu')
            new = []
    if len(log):
        log.to_csv(OUT / 'proba_log.csv', index=False)
    fin = log[log.status.isin(FINAL)] if len(log) else log
    rr = pd.to_numeric(fin['r'], errors='coerce') if len(fin) else pd.Series(dtype=float)
    kont = sum(1 for r in cand if r.get('proba_kontynuacja') and r.get('do_handlu'))   # V6.69: bez juz otwartych
    out = {'wystawione': int(len(log)), 'rozstrzygniete': int(len(fin)), 'sredni_R': rnd(rr.mean(), 3) if len(rr) else None,
           'suma_R': rnd(rr.sum(), 2) if len(rr) else None, 'teraz_do_handlu': len(new) + kont, 'kandydaci_teraz': len(cand),
           'nowe': len(new), 'kontynuacje': kont, 'aktywne_w_dzienniku': len(aktywne)}
    if blad_zapisu:
        out['blad_dziennika'] = 'zapis'
    rows = [{'grupa': 'wszystko', **out}]
    if len(fin):
        for (tv, sd), g in fin.groupby(['typ_wersja', 'side']):
            q = pd.to_numeric(g['r'], errors='coerce')
            rows.append({'grupa': f'{tv} {sd}', 'rozstrzygniete': len(g), 'sredni_R': rnd(q.mean(), 3), 'suma_R': rnd(q.sum(), 2)})
    pd.DataFrame(rows).to_csv(OUT / 'proba_wyniki.csv', index=False)
    return out


def _num(x):
    """Liczba skonczona albo None (V6.25: odrzuca NaN i +/-Infinity)."""
    if isinstance(x, bool):
        return None
    try:
        x = float(x)
    except (TypeError, ValueError):
        return None
    return x if math.isfinite(x) else None


def _licznik(x):
    """V6.25: licznik okazji - skonczona liczba calkowita >= 0, inaczej None."""
    v = _num(x)
    if v is None or v < 0 or abs(v - round(v)) > 1e-9:
        return None
    return int(round(v))


def proba_ok(r):
    """V6.22: czy setup typu w OBSERWACJI moze wejsc do proby. (True, '') albo (False, powod)."""
    if r.get('ocena') not in ('A', 'B'):
        return False, f"tryb probny: wymaga oceny A/B (jest {r.get('ocena')})"
    ok_n = _licznik(r.get('po_reg_okazje') if r.get('po_reg_okazje') is not None else 0)
    if ok_n is None:
        return False, f"tryb probny: nieprawidlowy licznik okazji ({r.get('po_reg_okazje')!r})"
    if ok_n < PROBA_MIN_OKAZJE and r.get('setup_type') in PROBA_Z_BADANIA:   # V6.60
        wyn = _num(r.get('po_reg_R'))
        if ok_n >= PROBA_BADANIE_STOP_N and (wyn is None or wyn < 0):
            return False, f"tryb probny (z badania): wynik na zywo {wyn}R < 0 przy {ok_n} okazjach"
        return True, ''
    if ok_n < PROBA_MIN_OKAZJE:
        return False, f"tryb probny: za malo okazji po ustaleniu regul ({ok_n} < {PROBA_MIN_OKAZJE})"
    e = _num(r.get('po_reg_edge'))
    if e is None or e <= 0:
        return False, f"tryb probny: przewaga po ustaleniu regul {e}R <= 0 ({ok_n} okazji)"
    t = _num(r.get('po_reg_t'))
    if PROBA_MAX_TYP_N_UJEMNY and ok_n >= PROBA_MAX_TYP_N_UJEMNY and (t is None or t < 0):
        return False, f"tryb probny: t przewagi {t} < 0 przy {ok_n} okazjach"
    wyn = _num(r.get('po_reg_R'))
    if PROBA_MIN_N_WYNIK and ok_n >= PROBA_MIN_N_WYNIK and (wyn is None or wyn < 0):
        return False, f"tryb probny: wynik netto po ustaleniu regul {wyn}R < 0 przy {ok_n} okazjach"
    return True, ''


def status_setupu(r):
    """V6.22: DO GRY / PROBA / OBSERWACJA / ZABLOKOWANY. V6.23: + BEZ BRAMKI (tryb 'etykieta')."""
    if r.get('do_handlu'):
        if r.get('tradfi_bez_blokad'):                 # V6.92
            return 'BEZ BRAMKI'
        if r.get('status_bramki') == 'DO GRY':
            return 'DO GRY'
        return 'PROBA' if BRAMKA_TRYB == 'probny' else 'BEZ BRAMKI'
    w = [x.strip() for x in str(r.get('warunki') or '').split(';') if x.strip()]
    if w and all(x.startswith('tryb probny') or x.startswith('bramka') or x.startswith('limit proby')
                 or x.startswith('limit ekspozycji') for x in w):   # V6.46
        return 'OBSERWACJA'
    return 'ZABLOKOWANY'


def pewnosc_przewagi(r):
    """V6.23: opis pewnosci przewagi typu+kierunku po ustaleniu regul (tylko informacja)."""
    n = _licznik(r.get('po_reg_okazje')) or 0
    t = _num(r.get('po_reg_t'))
    e = _num(r.get('po_reg_edge'))
    if n < PROBA_MIN_OKAZJE or e is None:
        return 'brak danych'
    if e <= 0:
        return 'brak przewagi'
    if n < 50 or t is None or t < 1:
        return 'niska'
    return 'wysoka' if t >= 2 else 'umiarkowana'


def test_bramki():
    """V6.23: syntetyczne przypadki we wszystkich trybach bramki + zgodnosc status_setupu z do_handlu."""
    global BRAMKA_TRYB
    stary = BRAMKA_TRYB
    wyniki = []
    try:
        for tryb in ('probny', 'twardy', 'etykieta'):
            BRAMKA_TRYB = tryb
            wyniki.append(_test_bramki_tryb(tryb))
    finally:
        BRAMKA_TRYB = stary
    return f"OK ({', '.join(wyniki)})"


def _test_bramki_tryb(tryb):
    base = {'symbol': 'TESTUSDT', 'side': 'LONG', 'setup_type': 'PULLBACK', 'entry_type': 'LIMIT', 'ocena': 'A',
            'status_bramki': 'OBSERWACJA', 'regime_1h': 'trend', 'btc_trend_1h': 1, 'corr_btc': 0.9, 'filtr': '',
            'tradable': True, 'po_reg_n': 60, 'po_reg_okazje': 60, 'po_reg_edge': 0.1, 'po_reg_t': 1.2, 'po_reg_R': 0.02}
    nie = {'aktywny': False, 'strony': {'LONG': {'aktywny': False}, 'SHORT': {'aktywny': False}}}
    tak = {'aktywny': False, 'strony': {'LONG': {'aktywny': True, 'sredni_R': -0.3}, 'SHORT': {'aktywny': False}}}
    odb = {'setup_type': 'ODBICIE', 'entry_type': 'MARKET', 'ocena': 'B', 'regime_1h': 'range', 'btc_trend_1h': -1}
    # oczekiwane do_handlu: (probny, twardy, etykieta)
    cases = [('0 okazji', {'po_reg_okazje': 0, 'po_reg_n': 0}, nie, None, (False, False, True)),
             ('19 okazji', {'po_reg_okazje': 19, 'po_reg_n': 19}, nie, None, (False, False, True)),
             ('20 okazji, przewaga > 0', {'po_reg_okazje': 20, 'po_reg_n': 25, 't': 0}, nie, None, (True, False, True)),
             ('50 okazji, przewaga > 0', {'po_reg_okazje': 50, 'po_reg_n': 50}, nie, None, (True, False, True)),
             ('przewaga ujemna', {'po_reg_edge': -0.05}, nie, None, (False, False, True)),
             ('przewaga zero', {'po_reg_edge': 0.0}, nie, None, (False, False, True)),
             ('brak danych przewagi', {'po_reg_edge': None}, nie, None, (False, False, True)),
             ('NaN przewagi', {'po_reg_edge': float('nan')}, nie, None, (False, False, True)),
             ('Infinity przewagi', {'po_reg_edge': float('inf')}, nie, None, (False, False, True)),
             ('nieprawidlowy licznik okazji', {'po_reg_okazje': -5}, nie, None, (False, False, True)),
             ('t < 0 przy 60 okazjach', {'po_reg_t': -0.4}, nie, None, (False, False, True)),
             ('wynik netto < 0 przy 60 okazjach (przewaga > 0)', {'po_reg_R': -0.11}, nie, None, (False, False, True)),
             ('wynik netto < 0 przy 30 okazjach (V6.52: blokada od 20)', {'po_reg_R': -0.11, 'po_reg_okazje': 30, 'po_reg_n': 30}, nie, None, (False, False, True)),
             ('wynik netto < 0 przy 15 okazjach (za malo okazji)', {'po_reg_R': -0.11, 'po_reg_okazje': 15, 'po_reg_n': 15}, nie, None, (False, False, True)),
             ('wynik netto 0.02 przy 25 okazjach', {'po_reg_R': 0.02, 'po_reg_okazje': 25, 'po_reg_n': 25}, nie, None, (True, False, True)),
             ('brak wyniku netto przy 60 okazjach', {'po_reg_R': None}, nie, None, (False, False, True)),
             ('wynik netto 0 przy 60 okazjach', {'po_reg_R': 0.0}, nie, None, (True, False, True)),
             ('BTC 1h przeciw, korelacja 0.9', {'btc_trend_1h': -1, 'corr_btc': 0.9}, nie, None, (False, False, False)),
             ('BTC 1h przeciw, korelacja 0 (V6.41)', {'btc_trend_1h': -1, 'corr_btc': 0.0}, nie, None, (True, False, True)),
             ('BTC 1h przeciw, brak korelacji', {'btc_trend_1h': -1, 'corr_btc': None}, nie, None, (False, False, False)),
             ('ocena C', {'ocena': 'C'}, nie, None, (False, False, True)),
             ('ocena D', {'ocena': 'D'}, nie, None, (False, False, False)),
             ('tryb ochronny LONG', {}, tak, None, (False, False, False)),
             ('tryb ochronny tylko LONG, setup SHORT', {'side': 'SHORT', 'btc_trend_1h': -1}, tak, None, (True, False, True)),
             ('typ DO GRY, 0 okazji, ocena C', {'status_bramki': 'DO GRY', 'po_reg_okazje': 0, 'ocena': 'C'}, nie, None, (True, True, True)),
             ('typ DO GRY + tryb ochronny', {'status_bramki': 'DO GRY'}, tak, None, (False, False, False)),
             ('alarm BTC', {}, nie, {'alarm': True}, (False, False, False)),
             ('filtr', {'filtr': 'koszt'}, nie, None, (False, False, False)),
             ('nie tradable', {'tradable': False}, nie, None, (False, False, False)),
             ('ilosc za mala, min. ryzyko 3x', {'ilosc_za_mala': True, 'ryzyko_min_usdt': 1.5, 'strata_sl_usdt': 0.5, 'status_bramki': 'DO GRY'}, nie, None, (False, False, False)),
             ('ilosc za mala, min. ryzyko 1.2x', {'ilosc_za_mala': True, 'ryzyko_min_usdt': 0.6, 'strata_sl_usdt': 0.5, 'status_bramki': 'DO GRY'}, nie, None, (True, True, True)),
             # V6.60: ODBICIE - PROBA z badania, bez blokady trybu ochronnego
             ('ODBICIE 0 okazji, tryb ochronny LONG', {**odb, 'po_reg_okazje': 0, 'po_reg_n': 0, 'po_reg_edge': None, 'po_reg_R': None}, tak, None, (True, False, True)),
             ('ODBICIE 12 okazji, wynik +0.3', {**odb, 'po_reg_okazje': 12, 'po_reg_n': 12, 'po_reg_R': 0.3}, nie, None, (True, False, True)),
             ('ODBICIE 12 okazji, wynik -0.2', {**odb, 'po_reg_okazje': 12, 'po_reg_n': 12, 'po_reg_R': -0.2}, nie, None, (False, False, True)),
             ('ODBICIE 25 okazji, przewaga ujemna', {**odb, 'po_reg_okazje': 25, 'po_reg_n': 25, 'po_reg_edge': -0.1}, nie, None, (False, False, True)),
             ('ODBICIE + alarm BTC', {**odb, 'po_reg_okazje': 0}, nie, {'alarm': True}, (False, False, False)),
             ('PULLBACK 0 okazji, tryb ochronny (bez wyjatku)', {'po_reg_okazje': 0, 'po_reg_n': 0}, tak, None, (False, False, False))]
    k = {'probny': 0, 'twardy': 1, 'etykieta': 2}[tryb]
    bad = []
    for name, over, och, shock, want in cases:
        r = {**base, **over}
        apply_conditions([r], shock, och, None)
        st = status_setupu(r)
        if bool(r['do_handlu']) != want[k]:
            bad.append(f"[{tryb}] {name}: do_handlu {r['do_handlu']} (oczekiwane {want[k]}) - {r['warunki']}")
        if (st in ('DO GRY', 'PROBA', 'BEZ BRAMKI')) != bool(r['do_handlu']):
            bad.append(f"[{tryb}] {name}: status_setupu {st} niezgodny z do_handlu {r['do_handlu']}")
        if r['do_handlu'] and st == 'DO GRY' and r.get('status_bramki') != 'DO GRY':
            bad.append(f"[{tryb}] {name}: DO GRY bez zatwierdzonego typu")
        if r['do_handlu'] and st == 'PROBA' and tryb != 'probny':
            bad.append(f"[{tryb}] {name}: PROBA poza trybem probnym")
    if bad:
        raise AssertionError('TEST BRAMKI NIEUDANY: ' + ' | '.join(bad))
    return f'{tryb} {len(cases)}'


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
        if OCHRONA_PER_STRONA and st.get('aktywny') and r.get('setup_type') not in OCHRONA_BEZ_TYPOW:   # V6.60
            why.append(f"tryb ochronny {r['side']} ({st.get('sredni_R')}R / {OCHRONA_DNI}d)")
        if r.get('setup_type') == 'SWING4H':
            if r.get('trend_4h') != s:
                why.append('trend 4h nie zgodny')
        elif r.get('setup_type') in ('CROWD', 'ODBICIE'):
            pass                                       # z zalozenia przeciw ruchowi - bez wymogu trendu (V6.53: + ODBICIE)
        elif r.get('regime_1h') != 'trend':
            why.append(f"rezim 1h {r.get('regime_1h')}")
        bt = r.get('btc_trend_1h')
        corr = _num(r.get('corr_btc'))
        if bt and bt == -s and r.get('setup_type') != 'ODBICIE' and (r['symbol'] == 'BTCUSDT' or (corr is None or corr >= BTC_CORR_MIN)):
            why.append('BTC 1h przeciw')
        if r.get('ocena') == 'D':
            why.append('ocena D')
        if r.get('ilosc_za_mala') and (_num(r.get('ryzyko_min_usdt')) or 0) > ILOSC_MIN_MAX_RYZYKO * (_num(r.get('strata_sl_usdt')) or 0):
            why.append(f"ilosc ponizej minimum kontraktu (min. ryzyko {r.get('ryzyko_min_usdt')} USDT)")   # V6.32
        if BRAMKA_TRYB == 'twardy' and r.get('status_bramki') != 'DO GRY':   # V6.17 (V6.23: bez flagi z startu)
            why.append(f"bramka {r.get('setup_type')} {r['side']}: OBSERWACJA (po ustaleniu regul {r.get('po_reg_n')} tr., sr. {r.get('po_reg_R')}R)")
        if BRAMKA_TRYB == 'probny' and r.get('status_bramki') not in ('DO GRY',) and r.get('ocena') != 'D':
            ok_p, why_p = proba_ok(r)                  # V6.22
            if not ok_p:
                why.append(why_p)
        if r.get('filtr'):
            why.append(f"filtr: {r['filtr']}")
        if not r.get('tradable', True):
            why.append('nie tradable')
        if TRADFI_BEZ_BLOKAD and r.get('asset_type') in TRADFI_TYPES:   # V6.93
            reason = str(r.get('not_tradable_reason') or '').strip()
            # Nie wolno dopuscic instrumentu z brakiem danych wykonania, zamknieta sesja,
            # przekroczonym spreadem/plynnoscia albo niespojnoscia ceny.
            execution_reasons = []
            if not r.get('tradable', True):
                execution_reasons.append('nie tradable' + (f' ({reason})' if reason else ' (brak potwierdzenia)'))
            elif any(flag in reason.lower() for flag in ('book/wolumen', 'cena niespojna', 'anomalia wolumenu', 'tradfi zamkniety')):
                execution_reasons.append(f'bezpieczenstwo wykonania ({reason})')
            if r.get('ilosc_za_mala'):
                min_risk = _num(r.get('ryzyko_min_usdt'))
                budget = _num(r.get('strata_sl_usdt'))
                if (min_risk is None or budget is None or budget <= 0 or
                        min_risk > ILOSC_MIN_MAX_RYZYKO * budget):
                    execution_reasons.append('minimum kontraktu: ryzyko przekracza limit lub dane sa niepelne')
            # Filtry statystyczne (ocena, rezim, BTC, bramka) informacyjne,
            # ale limit dzienny oraz blokady egzekucji sa twarde.
            hard = [x for x in why if x.startswith('dzienny limit')]
            hard.extend(execution_reasons)
            r['ostrzezenia_tradfi'] = '; '.join(x for x in why if x not in hard and x != 'nie tradable'
                                                and not x.startswith('ilosc ponizej minimum kontraktu'))
            r['blokady_wykonania_tradfi'] = '; '.join(execution_reasons)
            r['tradfi_bez_blokad'] = True
            why = hard
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
    d = d[~d.apply(lambda x: typ_wylaczony(x['setup_type'], x['side'], x.get('entry_type')), axis=1)] if len(d) else d
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
    elif r.get('setup_type') in BEZ_1R_TYPY:           # V6.53
        txt += f" | BEZ zamykania czesci na 1R: trzymaj do TP1 albo SL, max {SETUP_HOLD_H.get(r['setup_type'], BT_MAX_HOLD_H)}h"
    return txt + (f" | UWAGA: {r['dzwignia_uwagi']}" if r.get('dzwignia_uwagi') else '')


def print_report(good, live, bt_df, arch, gate, meta, top=(), htab=None):
    print('\n' + '=' * 90)
    print(f"{VERSION} | {meta['server_time'][:16]} UTC | kontrakty: krypto {meta.get('symbols_krypto')} + TradFi {meta.get('symbols_tradfi')} "
          f"| setupy {len(good)} (wszystkie {len(live)})")
    if not meta.get('drive') and not meta.get('replay'):   # V6.42
        print('!!! BRAK GOOGLE DRIVE - bramka, tryb ochronny i proby NIEWIARYGODNE (BRAK_DYSKU.txt) !!!')
    pb = meta.get('proba') or {}
    if pb:                                             # V6.20
        print(f"TRYB PROBNY: teraz {pb.get('teraz_do_handlu')} do handlu (z {pb.get('kandydaci_teraz')} kandydatow, max {PROBA_MAX_NA_RAZ}) | "
              f"dziennik proby: wystawione {pb.get('wystawione')}, rozstrzygniete {pb.get('rozstrzygniete')}, "
              f"sr. {pb.get('sredni_R')}R, suma {pb.get('suma_R')}R (proba_wyniki.csv)")
    rz = meta.get('rozbieznosci') or []
    if rz:                                             # V6.24
        print(f'UWAGA - ROZBIEZNOSC (wynik dodatni w probie projektowej, ujemny po ustaleniu regul) - {len(rz)} strategii:')
        for x in rz:
            print('  ' + x)
    jw = next((j for j in meta.get('jakosc_testu') or [] if str(j.get('zrodlo', '')).startswith('archiwum')), None)
    if jw and (_num(jw.get('po_ustaleniu_regul_sredni_R')) or 0) < 0:
        print(f"UWAGA: lacznie po ustaleniu regul {jw['po_ustaleniu_regul_sredni_R']}R ({jw['po_ustaleniu_regul_n']} tr.) - dodatnie wyniki "
              f"pojedynczych strategii w testach pochodza glownie z proby projektowej; o dopuszczeniu decyduje tylko bramka (po ustaleniu regul)")
    zk = meta.get('zakresy_testow') or {}
    if len(zk) > 1:                                    # V6.21
        print('UWAGA: testy nakladaja sie w czasie - to NIE sa niezalezne potwierdzenia: '
              + '; '.join(f'{k} {a[5:]} .. {b[5:]}' for k, (a, b) in zk.items()))
    for j in meta.get('jakosc_testu') or []:          # V6.16
        print(f"Jakosc testu [{j['zrodlo']}]: wykryte {j['wykryte']}, unikalne okazje {j['unikalne_okazje']}, rozstrzygniete "
              f"{j['rozstrzygniete']}, niejednoznaczne {j['niejednoznaczne']} ({j['niejednoznaczne_pct']}%), luka danych {j['z_luka_danych']} | "
              f"sr. R po filtrach: pesymistycznie {j['sredni_R_pesymistyczny']}, bez niejednoznacznych {j['sredni_R_bez_niejednoznacznych']} | "
              f"po filtrach {j['po_filtrach_n']} tr. = {j['unikalne_okazje_po_filtrach']} okazji z {j['niezalezne_zdjecia']} zdjec; "
              f"po ustaleniu regul {j['po_ustaleniu_regul_n']} tr. ({j['po_ustaleniu_regul_zdjecia']} zdjec), sr. {j['po_ustaleniu_regul_sredni_R']}R"
              + ('' if j['dane_flag'] == 'tak' else ' (stare wiersze bez oznaczen)'))
    if meta.get('odrzucone'):
        o = meta['odrzucone']
        print(f"Odrzucone przy budowie (odrzucone.csv): {o['n']} - " + ', '.join(f'{k} {v}' for k, v in list(o['powody'].items())[:5]))
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
        print(f'  {g.typ:16s} {g.side:5s} {g.status:10s} transakcji {g.transakcje:4d} ({g.unikalne_okazje} okazji, od {str(g.od)[5:16]})  sr. R {g.sredni_R}  przewaga {g.sredni_edge_R} '
              f't {g.t_edge}  (brakuje {g.brakuje_transakcji}) | bez filtrow: {g.bez_filtrow_transakcje} tr., sr. R {g.bez_filtrow_sredni_R}')
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
        print(f"  test akcji/indeksow (archiwum; od V6.88 cala doba): {sum(int(x.get('transakcje') or 0) for x in tt)} transakcji, "
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