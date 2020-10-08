#!/bin/bash
rm uuid.c uuid duuid 2>/dev/null
cython uuid.pyx --embed && gcc -Os -I/var/lang/include/python3.6m -o duuid uuid.c -lpython3.6m -lpthread -lm -lutil -ldl && staticx duuid uuid && strip uuid && echo 'OK'
rm uuid.c duuid 2>/dev/null
