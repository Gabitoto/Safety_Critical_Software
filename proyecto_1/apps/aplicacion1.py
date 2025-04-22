# Aplicación secundaria

def suma(a, b):
    # Precondición: a y b deben ser números
    assert type(a) in (int, float) and type(b) in (int, float)
    
    # Calcular la suma
    resultado = a + b
    
    # Postcondición: El resultado debe ser un número
    assert type(resultado) in (int, float)
    
    return resultado

# Ejemplo de uso
resultado = suma(2, 3)
print(resultado) # Salida: 5

# Ejemplo de error
# Intentar sumar un número y una cadena
# La sentencia assert generará un error
# resultado = suma(2, "3")