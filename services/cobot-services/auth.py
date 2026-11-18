import os
from fastapi import HTTPException, Security, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
import jwt

JWT_SECRET = os.getenv("JWT_SECRET", "um-segredo-de-desenvolvimento-com-mais-de-32-caracteres")
ALGORITHM = "HS256"

security = HTTPBearer(auto_error=False)

def validar_token_e_papel(credentials: HTTPAuthorizationCredentials = Security(security), papéis_permitidos: list = None):
    if not credentials:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Token de autenticação ausente",
            headers={"WWW-Authenticate": "Bearer"},
        )
    
    try:
        payload = jwt.decode(credentials.credentials, JWT_SECRET, algorithms=[ALGORITHM])
        user_role = payload.get("role")
        
        if papéis_permitidos and user_role not in papéis_permitidos:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=f"Perfil '{user_role}' não tem permissão para esta operação",
            )
        return payload
    except jwt.ExpiredSignatureError:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Token expirado")
    except jwt.PyJWTError:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Token inválido")