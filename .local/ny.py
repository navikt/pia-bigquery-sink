#!/usr/bin/env python3

import json
import subprocess
import uuid

from datetime import datetime
from random import SystemRandom


def generate_id() -> str:
    r = SystemRandom()
    return str(r.randint(9_000_000, 9_999_999))


def main() -> None:
    message = {
        '@id': str(uuid.uuid4()),
        '@opprettet': datetime.now().isoformat(),
        'eventName': 'hm-bigquery-sink-hendelse',
        'schemaId': 'hendelse_v1',
        'payload': {
            'navn': 'fordelingsresultat',
            'kilde': 'hm-saksberiker',
            'data': {
                'resultat': 'nyFlyt',
            },
        },
    }
    print("Melding:\n\n" + json.dumps(message, indent=2) + "\n")
    print("Sender melding...")
    subprocess.run(
        ['kafka-console-producer',
         '--broker-list', 'host.docker.internal:9092',
         '--topic', 'teamdigihot.hm-soknadsbehandling-v1'],
        input=json.dumps(message), encoding='utf-8'
    )


main()
