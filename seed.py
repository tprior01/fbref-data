import psycopg2
from configparser import ConfigParser

comps = {"ENG": "Premier-League",
         "ITA": "Serie-A",
         "FRA": "Ligue-1",
         "SPA": "La-Liga",
         "GER": "Bundesliga",
         "CL": "Champions-League",
         "EL": "Europa-League"}
seasons = [23, 22, 21, 20, 19, 18]


def config(filename='database.ini', section='postgresql'):
    # create a parser
    parser = ConfigParser()
    # read config file
    parser.read(filename)
    # get section, default to postgresql
    db = {}
    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            db[param[0]] = param[1]
    else:
        raise Exception(f"Section {section} not found in the {filename} file")
    return db


drop_all_tables = """DROP SCHEMA public CASCADE;
                     CREATE SCHEMA public;"""

player_info = """CREATE TABLE player_info (id VARCHAR(30) PRIMARY KEY, name VARCHAR(255) NOT NULL, dob DATE NOT NULL,
                 nation VARCHAR(255), club VARCHAR(255), main_position VARCHAR(20), other_position VARCHAR(20),
                 current_value DECIMAL(4,1), max_value DECIMAL(4,1))"""


def keeper(comp, season):
    return f"""CREATE TABLE {comp}{season}KEEPER (id VARCHAR(30) PRIMARY KEY, goals_ag SMALLINT, sot_ag SMALLINT, 
               saves SMALLINT, gk_won SMALLINT, gk_drew SMALLINT, gk_lost SMALLINT, cs SMALLINT, pk_ag SMALLINT,
               pk_not_saved SMALLINT, pk_saved SMALLINT, pk_missed SMALLINT)"""


def copy_keeper(comp, season):
    return f"""COPY {comp}{season}keeper(id, goals_ag, sot_ag, saves, gk_won, gk_drew, gk_lost, cs, pk_ag, pk_not_saved, 
              pk_saved, pk_missed)
              FROM '/Users/tom/Documents/Personal/fbref-4/data/{comps[comp]}/keepers/20{season-1}-20{season}.csv'
              DELIMITER ','
              CSV HEADER;"""


def keeperadv(comp, season):
    return f"""CREATE TABLE {comp}{season}KEEPERADV (id VARCHAR(30) PRIMARY KEY, fk_ag SMALLINT, ck_ag SMALLINT, 
               og_ag SMALLINT, ps_xG DECIMAL(5,1), ps_xG_delta DECIMAL(5,1), cmp_launch SMALLINT, att_launch SMALLINT,
               att_gk_pass SMALLINT, att_gk_throw SMALLINT, att_dead_launch SMALLINT, att_cross_ag SMALLINT, 
               stop_cross_ag SMALLINT, def_outside_pen SMALLINT, gk_pass_len DECIMAL(7,1), gk_dead_pass_len DECIMAL(7,1),
               def_outside_pen_dist DECIMAL(7,1))"""


def copy_keeperadv(comp, season):
    return f"""COPY {comp}{season}keeperadv(id, fk_ag, ck_ag, og_ag, ps_xG, ps_xG_delta, cmp_launch, att_launch, 
               att_gk_pass, att_gk_throw, att_dead_launch, att_cross_ag, stop_cross_ag, def_outside_pen, gk_pass_len, 
               gk_dead_pass_len, def_outside_pen_dist)
               FROM '/Users/tom/Documents/Personal/fbref-4/data/{comps[comp]}/keepersadv/20{season-1}-20{season}.csv'
               DELIMITER ','
               CSV HEADER;"""


def shooting(comp, season):
    return f"""CREATE TABLE {comp}{season}SHOOTING (
               id VARCHAR(30) PRIMARY KEY, goals SMALLINT, shot SMALLINT, shot_on_target SMALLINT, fk SMALLINT, 
               pk SMALLINT, att_pk SMALLINT, xG DECIMAL(6,1), npxG DECIMAL(6,1), G__xG DECIMAL(6,1), 
               npG__npxG DECIMAL(6,1), tot_shot_dist DECIMAL(7,1))"""


def copy_shooting(comp, season):
    return f"""COPY {comp}{season}shooting(id, goals, shot, shot_on_target, fk, pk, att_pk, xG, npxG, G__xG, npG__npxG, 
              tot_shot_dist)
              FROM '/Users/tom/Documents/Personal/fbref-4/data/{comps[comp]}/shooting/20{season-1}-20{season}.csv'
              DELIMITER ','
              CSV HEADER;"""


def passing(comp, season):
    return f"""CREATE TABLE {comp}{season}PASSING (id VARCHAR(30) PRIMARY KEY, cmp_pass SMALLINT, att_pass SMALLINT, 
               tot_pass_dist DECIMAL(7,1), prog_pass_dist DECIMAL(7,1), cmp_pass_def SMALLINT, att_pass_def SMALLINT,
               cmp_pass_mid SMALLINT, att_pass_mid SMALLINT, cmp_pass_att SMALLINT, att_pass_att SMALLINT, 
               assist SMALLINT, xAG DECIMAL(6,1), xA DECIMAL(6,1), A__xAG DECIMAL(6,1), key_pass SMALLINT, 
               fin_3rd_pass SMALLINT, opp_pen_pass SMALLINT, acc_cross SMALLINT, prog_pass SMALLINT)"""


def copy_passing(comp, season):
    return f"""COPY {comp}{season}passing(id, cmp_pass, att_pass, tot_pass_dist, prog_pass_dist, cmp_pass_def, 
               att_pass_def, cmp_pass_mid, att_pass_mid, cmp_pass_att, att_pass_att, assist, xAG, xA, A__xAG,
               key_pass, fin_3rd_pass, opp_pen_pass, acc_cross, prog_pass)
               FROM '/Users/tom/Documents/Personal/fbref-4/data/{comps[comp]}/passing/20{season-1}-20{season}.csv'
               DELIMITER ','
               CSV HEADER;"""


def passing_types(comp, season):
    return f"""CREATE TABLE {comp}{season}PASSING_TYPES (id VARCHAR(30) PRIMARY KEY, live_pass SMALLINT, 
               dead_pass SMALLINT, fk_pass SMALLINT, tb_pass SMALLINT, sw_pass SMALLINT, cross_pass SMALLINT, 
               throw_in SMALLINT, ck SMALLINT, ck_in SMALLINT, ck_out SMALLINT, ck_straight SMALLINT, 
               offside_pass SMALLINT, blocked_pass SMALLINT)"""


def copy_passing_types(comp, season):
    return f"""COPY {comp}{season}passing_types(id, live_pass, dead_pass, fk_pass, tb_pass, sw_pass, cross_pass, 
               throw_in, ck, ck_in, ck_out, ck_straight, offside_pass, blocked_pass)
               FROM '/Users/tom/Documents/Personal/fbref-4/data/{comps[comp]}/passing_types/20{season-1}-20{season}.csv'
               DELIMITER ','
               CSV HEADER;"""


def gca(comp, season):
    return f"""CREATE TABLE {comp}{season}GCA (id VARCHAR(30) PRIMARY KEY, sca SMALLINT, sca_pass_live SMALLINT, 
               sca_pass_dead SMALLINT, sca_take_on SMALLINT, sca_shot SMALLINT, sca_fouled SMALLINT, sca_def SMALLINT,
               gca SMALLINT, gca_pass_live SMALLINT, gca_pass_dead SMALLINT, gca_take_on SMALLINT, gca_shot SMALLINT, 
               gca_fouled SMALLINT, gca_def SMALLINT)"""


def copy_gca(comp, season):
    return f"""COPY {comp}{season}gca(id, sca, sca_pass_live, sca_pass_dead, sca_take_on, sca_shot, sca_fouled, sca_def,
               gca, gca_pass_live, gca_pass_dead, gca_take_on, gca_shot, gca_fouled, gca_def)
               FROM '/Users/tom/Documents/Personal/fbref-4/data/{comps[comp]}/gca/20{season-1}-20{season}.csv'
               DELIMITER ','
               CSV HEADER;"""


def defense(comp, season):
    return f"""CREATE TABLE {comp}{season}defense (id VARCHAR(30) PRIMARY KEY, att_tackle SMALLINT, cmp_tackle SMALLINT, 
               att_tackle_def SMALLINT, att_tackle_mid SMALLINT, att_tackle_att SMALLINT, att_drib_tackle SMALLINT, 
               cmp_drib_tackle SMALLINT, uns_drib_tackle SMALLINT, block SMALLINT, block_shot SMALLINT, 
               block_pass SMALLINT, intercept SMALLINT, tackle_intercept SMALLINT, clearance SMALLINT, error SMALLINT,
               recov SMALLINT)"""


def copy_defense(comp, season):
    return f"""COPY {comp}{season}defense(id, att_tackle, cmp_tackle, att_tackle_def, att_tackle_mid, att_tackle_att, 
               att_drib_tackle, cmp_drib_tackle, uns_drib_tackle, block, block_shot, block_pass, intercept, 
               tackle_intercept, clearance, error, recov)
               FROM '/Users/tom/Documents/Personal/fbref-4/data/{comps[comp]}/defense/20{season-1}-20{season}.csv'
               DELIMITER ','
               CSV HEADER;"""


def possession(comp, season):
    return f"""CREATE TABLE {comp}{season}possession (id VARCHAR(30) PRIMARY KEY, touch SMALLINT, touch_def_pen SMALLINT, 
               touch_def SMALLINT, touch_mid SMALLINT, touch_att SMALLINT, touch_att_pen SMALLINT,  touch_live SMALLINT, 
               att_take_on SMALLINT, cmp_take_on SMALLINT, uns_take_on SMALLINT, carry SMALLINT, carry_dist DECIMAL(7,1), 
               carry_prog_dist DECIMAL(7,1), carry_prog SMALLINT, carry_att_third SMALLINT, carry_opp_pen SMALLINT,
               miscontrol SMALLINT, disposs SMALLINT, received SMALLINT, prog_received SMALLINT)"""


def copy_possession(comp, season):
    return f"""COPY {comp}{season}possession(id, touch, touch_def_pen, touch_def, touch_mid, touch_att, touch_att_pen, 
               touch_live, att_take_on, cmp_take_on, uns_take_on, carry, carry_dist, carry_prog_dist, carry_prog, 
               carry_att_third, carry_opp_pen, miscontrol, disposs, received, prog_received)
               FROM '/Users/tom/Documents/Personal/fbref-4/data/{comps[comp]}/possession/20{season-1}-20{season}.csv'
               DELIMITER ','
               CSV HEADER;"""


def playingtime(comp, season):
    return f"""CREATE TABLE {comp}{season}playingtime (id VARCHAR(30) PRIMARY KEY, matches SMALLINT, minutes SMALLINT, 
               nineties SMALLINT, starts SMALLINT, completed SMALLINT, sub SMALLINT,  sub_unused SMALLINT, 
               onpitch_goals SMALLINT, onpitch_goals_ag SMALLINT, onpitch_goals_delta SMALLINT, onpitch_xG DECIMAL(6,1), 
               onpitch_xGA DECIMAL(6,1), onpitch_xG_delta DECIMAL(6,1), matches_gk SMALLINT, minutes_gk SMALLINT, starts_gk SMALLINT, 
               nineties_gk SMALLINT)"""


def copy_playingtime(comp, season):
    return f"""COPY {comp}{season}playingtime(id, matches, minutes, nineties, starts, completed, sub,  sub_unused, 
               onpitch_goals, onpitch_goals_ag, onpitch_goals_delta, onpitch_xG, onpitch_xGA, onpitch_xG_delta, 
               matches_gk, minutes_gk, starts_gk, nineties_gk)
               FROM '/Users/tom/Documents/Personal/fbref-4/data/{comps[comp]}/playingtime/20{season-1}-20{season}.csv'
               DELIMITER ','
               CSV HEADER;"""


def misc(comp, season):
    return f"""CREATE TABLE {comp}{season}misc (id VARCHAR(30) PRIMARY KEY, y_card SMALLINT, r_card SMALLINT, 
               two_y_card SMALLINT, fouls SMALLINT, fouled SMALLINT, offside SMALLINT,  pens_won SMALLINT, 
               pens_con SMALLINT, own_goal SMALLINT)"""


def copy_misc(comp, season):
    return f"""COPY {comp}{season}misc(id, y_card, r_card, two_y_card, fouls, fouled, offside,  pens_won, pens_con, 
               own_goal)
               FROM '/Users/tom/Documents/Personal/fbref-4/data/{comps[comp]}/misc/20{season-1}-20{season}.csv'
               DELIMITER ','
               CSV HEADER;"""


create = [keeper, keeperadv, shooting, passing, passing_types, gca, defense, possession, playingtime, misc]
copy = [copy_keeper, copy_keeperadv, copy_shooting, copy_passing, copy_passing_types, copy_gca, copy_defense, copy_possession, copy_playingtime, copy_misc]


def execute_commands(*coms):
    """ create tables in the postgresql database"""
    conn = None
    try:
        # read the connection parameters
        params = config()
        # connect to the postgresql server
        conn = psycopg2.connect(**params)
        cur = conn.cursor()
        # create table one by one
        for command in coms:
            cur.execute(command)
        # close communication with the postgresql database server
        cur.close()
        # commit the changes
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()


if __name__ == '__main__':
    execute_commands(drop_all_tables)
    create_tables = (
        player_info,
        *[func(comp, season) for comp in comps.keys() for season in seasons for func in create],
    )
    execute_commands(*create_tables)
    copy_tables = (
        *[func(comp, season) for comp in comps.keys() for season in seasons for func in copy],
    )
    execute_commands(*copy_tables)
