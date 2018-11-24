import os
import psycopg2
from uuid import UUID
from flask import Flask, jsonify
from flask import abort
from flask import request

app = Flask(__name__)
params = {'host': os.environ.get('HOST'), 'database': os.environ.get('DB'), 'user': os.environ.get('USER'), 'password': os.environ.get('PASS'), 'port': '5432'}

SELECT_PARTY_BY_USER_ID = """SELECT s.real_name, s.name, p.real_name 
FROM santa as s LEFT JOIN santa as p on (s.party = p.name)
WHERE s.uuid = %s"""
SELECT_PARTY_BY_USER_NAME = """SELECT s.uuid, s.name, s.party, p.real_name
FROM santa as s LEFT JOIN santa as p on (s.party = p.name)
WHERE s.name = %s"""
SELECT_CANDIDATE = """SELECT name, real_name FROM santa WHERE has_party ISNULL AND name != %s
        ORDER BY uuid LIMIT 1"""
UPDATE_PARTY = """UPDATE santa SET party = %s WHERE name = %s"""
UPDATE_CANDIDATE = """UPDATE santa SET has_party = true WHERE name = %s"""

def get_party_full_name(user):
    conn = None
    try:
        conn = psycopg2.connect(**params)
        cur = conn.cursor()
        cur.execute(SELECT_PARTY_BY_USER_NAME, (user,))
        party = cur.fetchone()
        return party
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        cur.close()
        if conn is not None:
            conn.close()

def get_candidate_for_santa(user):
    conn = None
    try:
        conn = psycopg2.connect(**params)
        cur = conn.cursor()
        cur.execute(SELECT_CANDIDATE, (user,))
        candidate = cur.fetchone()
        return candidate
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        cur.close()
        if conn is not None:
            conn.close()

def assign_candidate(candidate, user):
    conn = None
    try:
        conn = psycopg2.connect(**params)
        cur = conn.cursor()
        cur.execute(UPDATE_PARTY, (candidate, user))
        cur.execute(UPDATE_CANDIDATE, (candidate,))
        conn.commit()
        return None
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        cur.close()
        if conn is not None:
            conn.close()

def get_party_by_user_id(uuid):
    conn = None
    try:
        conn = psycopg2.connect(**params)
        cur = conn.cursor()
        cur.execute(SELECT_PARTY_BY_USER_ID, (uuid,))
        cur_party=cur.fetchone()
        return cur_party
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        cur.close()
        if conn is not None:
            conn.close()

@app.route("/", methods=['POST'])
def postparty():

    # Logs
    print('Request:', request.json)

    assert request.json, abort(400)
    assert 'name' in request.json, abort(400)

    party = get_party_full_name(request.json['name'])

    # Logs
    print('Party:', party)

    assert len(party), abort(404)

    if party[3]:
        resp = jsonify({'party': party[3],
                        "your_uuid": party[0]})
    else:
        candidate = get_candidate_for_santa(request.json['name'])

        assert len(candidate), abort(404)

        try:
            assign_candidate(candidate[0], request.json['name'])
        except Exception as e:
            print(e)
            abort(400)
        resp = jsonify({'party': candidate[1],
                        "your_uuid": party[0]})
    return resp


@app.route("/<uuid>", methods=['GET'])
def getparty(uuid):
    assert uuid, abort(400)

    try:
        UUID(uuid, version=4)
    except ValueError:
        abort(400)

    party = get_party_by_user_id(uuid)
    assert len(party), abort(404)

    return jsonify({'user_name': party[0],
                    'party_name': party[2],
                    'user': party[1]
                   })


if __name__ == '__main__':
    port = int(os.environ.get("PORT", 5000))
    app.run(host='0.0.0.0', port=port)