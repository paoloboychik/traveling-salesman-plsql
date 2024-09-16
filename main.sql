CREATE TABLE cities(
    aid NUMBER NOT NULL,
    acity VARCHAR2(40) NOT NULL,
    bid NUMBER NOT NULL,
    bcity VARCHAR2(40) NOT NULL,
    dist NUMBER,
    CONSTRAINT cities_check_id CHECK(aid != bid AND aid >= 1 AND bid >= 1),
    CONSTRAINT cities_check_dist CHECK(dist IS NULL OR dist >= 0)
);
/
DROP TABLE cities;
/
TRUNCATE TABLE cities;
/
INSERT ALL
  INTO cities VALUES(1, 'Moscow', 2, 'Tokyo', NULL)
  INTO cities VALUES(1, 'Moscow', 3, 'Paris', NULL)
  INTO cities VALUES(1, 'Moscow', 4, 'Dublin', 1)
  INTO cities VALUES(1, 'Moscow', 5, 'Berlin', 2)
  INTO cities VALUES(2, 'Tokyo', 3, 'Paris', NULL)
  INTO cities VALUES(2, 'Tokyo', 4, 'Dublin', 3)
  INTO cities VALUES(2, 'Tokyo', 5, 'Berlin', 6)
  INTO cities VALUES(3, 'Paris', 4, 'Dublin', 4)
  INTO cities VALUES(3, 'Paris', 5, 'Berlin', 2)
  INTO cities VALUES(4, 'Dublin', 5, 'Berlin', 7)
(SELECT 1
FROM dual);
/
INSERT ALL
    INTO cities VALUES(1, 'Moscow', 2, 'Tokyo', 5)
    INTO cities VALUES(1, 'Moscow', 2, 'Tokyo', NULL)
    INTO cities VALUES(1, 'Moscow', 3, 'Paris', 3)
    INTO cities VALUES(2, 'Tokyo', 3, 'Paris', 5)
(SELECT 1
FROM dual);
/
SELECT * FROM cities;
/
COMMIT;
/
CREATE OR REPLACE PACKAGE ind3
IS
    TYPE cities_rec_type IS RECORD(
        aid cities.aid%TYPE, 
        bid cities.bid%TYPE, 
        dist cities.dist%TYPE
    );
    TYPE cities_type IS TABLE OF
        cities_rec_type
        INDEX BY PLS_INTEGER;
    TYPE distinct_rec_type IS RECORD(
        id cities.aid%TYPE,
        city cities.acity%TYPE
    );
    TYPE distinct_type IS TABLE OF
        distinct_rec_type
        INDEX BY PLS_INTEGER;
    TYPE id_type IS TABLE OF
        cities.aid%TYPE
        INDEX BY PLS_INTEGER;
    TYPE combs_type IS TABLE OF
        VARCHAR2(40)
        INDEX BY PLS_INTEGER;
    TYPE routs_rec_type IS RECORD(
        rout VARCHAR2(40),
        dist cities.dist%TYPE
    );
    TYPE routs_type IS TABLE OF
        routs_rec_type
        INDEX BY PLS_INTEGER;
    FUNCTION check_id(p_tab distinct_type) RETURN BOOLEAN;
    FUNCTION check_repet(p_tab cities_type) RETURN BOOLEAN;
    FUNCTION check_pres(p_tab_dist distinct_type, p_tab_cit cities_type) 
        RETURN BOOLEAN;
    FUNCTION check_isolate(p_tab cities_type) RETURN BOOLEAN;
    FUNCTION check_queue(p_tab distinct_type) RETURN BOOLEAN;
    PROCEDURE combs(
        p_cur_str IN VARCHAR2,
        p_cur_arr IN id_type,
        p_i IN OUT PLS_INTEGER,
        p_res_arr IN OUT combs_type
    );
    FUNCTION dist_calc(p_rout VARCHAR2, p_tab cities_type) 
        RETURN cities.dist%TYPE;
    PROCEDURE output_res(
        p_min_len cities.dist%TYPE, 
        p_tab_routs routs_type,
        p_tab_distinct distinct_type
    );
    PROCEDURE main;
END ind3;
/
CREATE OR REPLACE PACKAGE BODY ind3
IS
    FUNCTION check_id(p_tab distinct_type) 
        RETURN BOOLEAN
        IS
        BEGIN
            <<outer_loop>>
            FOR i IN 1..p_tab.COUNT-1 LOOP
                <<inner_loop>>
                FOR j IN 1+i..p_tab.COUNT LOOP
                    IF p_tab(i).id = p_tab(j).id THEN
                        RETURN TRUE;
                    END IF;
                END LOOP inner_loop;
            END LOOP outer_loop;
            RETURN FALSE;
        END check_id;
    FUNCTION check_repet(p_tab cities_type) 
        RETURN BOOLEAN
        IS
        BEGIN
            <<outer_loop>>
            FOR i IN 1..p_tab.COUNT-1 LOOP
                <<inner_loop>>
                FOR j IN 1+i..p_tab.COUNT LOOP
                    IF p_tab(i).aid = p_tab(j).aid 
                        AND p_tab(i).bid = p_tab(j).bid
                        OR p_tab(i).aid = p_tab(j).bid 
                        AND p_tab(i).bid = p_tab(j).aid 
                    THEN
                        IF NVL(p_tab(i).dist, -1) 
                            != NVL(p_tab(j).dist, -1)
                        THEN 
                            RETURN TRUE;
                    END IF;
                    END IF;
                END LOOP inner_loop;
            END LOOP outer_loop;
            RETURN FALSE;
        END check_repet;
    FUNCTION check_pres(
        p_tab_dist distinct_type,
        p_tab_cit cities_type
    ) 
        RETURN BOOLEAN
        IS
            v_tab_cit cities_type;
            v_count NUMBER;
            v_i NUMBER;
        BEGIN
            v_count := p_tab_dist.COUNT;
            v_i := 0;
            <<outer_loop>>
            FOR i IN 1..v_count-1 LOOP
                <<inner_loop>>
                FOR j IN 1+i..v_count LOOP
                    v_i := v_i + 1;
                    v_tab_cit(v_i).aid := p_tab_dist(i).id;
                    v_tab_cit(v_i).bid := p_tab_dist(j).id;
                END LOOP inner_loop;
            END LOOP outer_loop;
            <<outer_loop>>
            FOR i IN v_tab_cit.FIRST..v_tab_cit.LAST LOOP
                <<inner_loop>>
                FOR j IN p_tab_cit.FIRST..p_tab_cit.LAST LOOP
                    IF v_tab_cit(i).aid = p_tab_cit(j).aid 
                        AND v_tab_cit(i).bid = p_tab_cit(j).bid
                        OR v_tab_cit(i).aid = p_tab_cit(j).bid 
                        AND v_tab_cit(i).bid = p_tab_cit(j).aid 
                        THEN
                            v_tab_cit.DELETE(i);
                            EXIT inner_loop;
                    END IF;
                END LOOP inner_loop;
            END LOOP outer_loop;
            IF v_tab_cit.COUNT = 0 THEN 
                RETURN FALSE;
            ELSE 
                RETURN TRUE;
            END IF;
        END;
    FUNCTION check_isolate(p_tab cities_type) 
        RETURN BOOLEAN
        IS
            v_bool BOOLEAN := TRUE;
        BEGIN
            RETURN FALSE;
        END check_isolate;    
    FUNCTION check_queue(p_tab distinct_type) 
        RETURN BOOLEAN 
        IS
            v_bool BOOLEAN := FALSE;
            v_i PLS_INTEGER;
            v_n NUMBER := 0;
        BEGIN
            v_i := p_tab.FIRST;
            LOOP
                v_n := v_n + 1;
                IF p_tab(v_i).id != v_n
                    THEN 
                        v_bool := TRUE;
                        EXIT;
                    END IF;
                v_i := p_tab.NEXT(v_i);
                EXIT WHEN v_i IS NULL;
            END LOOP;
            RETURN v_bool;
        END check_queue;
    PROCEDURE combs(
        p_cur_str IN VARCHAR2,
        p_cur_arr IN id_type,
        p_i IN OUT PLS_INTEGER,
        p_res_arr IN OUT combs_type
    )
        IS
            v_cur_str VARCHAR2(40);
            v_cur_arr id_type;
            v_i PLS_INTEGER;
        BEGIN
            v_i := p_cur_arr.FIRST;
            LOOP
                v_cur_str := p_cur_str || p_cur_arr(v_i) || ' ';
                v_cur_arr := p_cur_arr;
                v_cur_arr.DELETE(v_i);
                IF v_cur_arr.COUNT = 0
                    THEN 
                        p_i := p_i + 1;
                        p_res_arr(p_i) := v_cur_str || '1';
                    ELSE
                        combs(v_cur_str, v_cur_arr, p_i, p_res_arr);
                    END IF;
                v_i := p_cur_arr.NEXT(v_i);
                EXIT WHEN v_i IS NULL;
            END LOOP;
        END combs;
    FUNCTION dist_calc(p_rout VARCHAR2, p_tab cities_type) 
        RETURN cities.dist%TYPE
        IS
            v_count NUMBER;
            v_dist cities.dist%TYPE := 0;
        BEGIN
            v_count := REGEXP_COUNT(p_rout, '\d+');
            FOR i IN 1..v_count-1 LOOP
                FOR j IN p_tab.FIRST..p_tab.LAST LOOP
                    IF p_tab(j).aid = REGEXP_SUBSTR(p_rout, '\d+', 1, i)
                        AND p_tab(j).bid = REGEXP_SUBSTR(p_rout, '\d+', 1, i+1)
                        OR p_tab(j).bid = REGEXP_SUBSTR(p_rout, '\d+', 1, i)
                        AND p_tab(j).aid = REGEXP_SUBSTR(p_rout, '\d+', 1, i+1)
                        THEN v_dist := v_dist + p_tab(j).dist;
                        EXIT;
                        END IF;
                END LOOP;
            END LOOP;
            RETURN v_dist;
        END dist_calc;
    PROCEDURE output_res(
        p_min_len cities.dist%TYPE, 
        p_tab_routs routs_type,
        p_tab_distinct distinct_type
    )
        IS
            v_count NUMBER;
            v_city cities.acity%TYPE;
        BEGIN
            FOR i IN p_tab_routs.FIRST..p_tab_routs.LAST LOOP
                IF p_tab_routs(i).dist = p_min_len
                    THEN
                        v_count := REGEXP_COUNT(p_tab_routs(i).rout, '\d+');
                        FOR j IN 1..v_count LOOP
                            FOR k IN p_tab_distinct.FIRST..p_tab_distinct.LAST LOOP
                                IF p_tab_distinct(k).id = REGEXP_SUBSTR(p_tab_routs(i).rout, '\d+', 1, j)
                                    THEN 
                                        v_city := p_tab_distinct(k).city;
                                        EXIT;
                                    END IF;
                            END LOOP;
                            DBMS_OUTPUT.PUT(v_city);
                            IF j != v_count THEN
                                DBMS_OUTPUT.PUT('->');
                                END IF;
                        END LOOP;
                        DBMS_OUTPUT.NEW_LINE;
                        DBMS_OUTPUT.PUT_LINE('The total length of the route: ' || p_min_len);
                    END IF;
            END LOOP;
        END;
    PROCEDURE main
    IS
        CURSOR c_cities IS 
            SELECT aid, bid, dist FROM cities;
        CURSOR c_distinct IS 
            SELECT DISTINCT aid, acity FROM cities
            UNION
            SELECT DISTINCT bid, bcity FROM cities
            ORDER BY 1;
        v_cities ind3.cities_type;
        v_distinct ind3.distinct_type;
        v_combs ind3.combs_type;
        v_ids ind3.id_type;
        v_routs ind3.routs_type;
        v_i PLS_INTEGER;
        v_len NUMBER;
        /*исключения*/
        e_empty_table EXCEPTION;
        e_check_id EXCEPTION;
        e_check_repet EXCEPTION;
        e_check_pres EXCEPTION;
        e_check_isolate EXCEPTION;
        e_check_queue EXCEPTION;
        e_check_routs EXCEPTION;
    BEGIN
        v_i := 0;
        FOR i IN c_cities LOOP
            v_i := v_i + 1;
            v_cities(v_i) := i;
        END LOOP;
        v_i := 0;
        FOR i IN c_distinct LOOP
            v_i := v_i + 1;
            v_distinct(v_i) := i;
        END LOOP;
    
        IF v_distinct.COUNT = 0 
            THEN RAISE e_empty_table;
            END IF;
        IF ind3.check_id(v_distinct) 
            THEN RAISE e_check_id;
            END IF;
        IF ind3.check_repet(v_cities) 
            THEN RAISE e_check_repet;
            END IF;
        IF ind3.check_pres(v_distinct, v_cities)
            THEN RAISE e_check_pres;
            END IF;
        IF ind3.check_isolate(v_cities)
            THEN RAISE e_check_isolate;
            END IF;
        IF ind3.check_queue(v_distinct)
            THEN RAISE e_check_queue;
            END IF;
      
        v_i := 0;
        FOR i IN v_distinct.FIRST..v_distinct.LAST LOOP
            IF v_distinct(i).id != 1 THEN
                v_i := v_i + 1;
                v_ids(v_i) := v_distinct(i).id;
                END IF;
        END LOOP;
        
        v_i := 0;
        ind3.combs('1 ', v_ids, v_i, v_combs);
        
        v_i := 0;
        FOR i IN v_combs.FIRST..v_combs.LAST LOOP
            IF ind3.dist_calc(v_combs(i), v_cities) IS NOT NULL THEN
                v_i := v_i + 1;
                v_routs(v_i).rout := v_combs(i);
                v_routs(v_i).dist := ind3.dist_calc(v_routs(v_i).rout, v_cities);
                END IF;
        END LOOP;
        
        IF v_routs.COUNT = 0
            THEN RAISE e_check_routs;
            END IF;
            
        v_len := -1;
        FOR i IN v_routs.FIRST..v_routs.LAST LOOP
            IF v_len > v_routs(i).dist OR v_len = -1 
                THEN v_len := v_routs(i).dist;
                END IF;
        END LOOP;
        
        ind3.output_res(v_len, v_routs, v_distinct);
    EXCEPTION
        WHEN e_empty_table THEN
            DBMS_OUTPUT.PUT_LINE('Empty table!');
        WHEN e_check_id THEN
            DBMS_OUTPUT.PUT_LINE('Several cities correspond to one index!');
        WHEN e_check_repet THEN
            DBMS_OUTPUT.PUT_LINE('Several distances correspond to couple of cities!');
        WHEN e_check_pres THEN
            DBMS_OUTPUT.PUT_LINE('Insufficient data!');
        WHEN e_check_isolate THEN
            DBMS_OUTPUT.PUT_LINE('Isolated city!');
        WHEN e_check_queue THEN
            DBMS_OUTPUT.PUT_LINE('Incorrect numbering (not 1..n)!');
        WHEN e_check_routs THEN
            DBMS_OUTPUT.PUT_LINE('Cities cannot be connected by a closed route!');
            DBMS_OUTPUT.PUT_LINE('(There are some isolated groups of cities)');
        WHEN OTHERS THEN
            DBMS_OUTPUT.PUT_LINE('Unknown error!');
    END main;
END ind3;
/
SET SERVEROUTPUT ON;
SET VERIFY OFF;
EXEC ind3.main;
