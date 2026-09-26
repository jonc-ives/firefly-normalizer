from abc import ABC, abstractmethod
from collections.abc import AsyncGenerator, Callable
from contextlib import AsyncExitStack, asynccontextmanager
from dataclasses import dataclass
from fastapi import APIRouter, FastAPI
from typing import Any, ClassVar, final

import uvicorn


@dataclass
class WebhookConfig:
    uvi_host: str = "127.0.0.1"
    uvi_port: int = 8000
    uvi_level: str = "error"
    uvi_reload: bool = False
    uvi_workers: int = 1
    uvi_proxy_headers: bool = True
    uvi_forwarded_allow_ips: str = "127.0.0.1"
    uvi_timeout_keep_alive: int = 5
    uvi_access_log: bool = True

    api_title: str = "webhook"
    api_version: str = "0.1.0"
    api_description: str = ""
    api_prefix: str = ""

    __prefix_tail: ClassVar[str] = "prefix must not end with a trailing '/'"
    __prefix_head: ClassVar[str] = "prefix must begin with a '/'"
    __workers_neg: ClassVar[str] = "worker count must be greater than 0"
    __port_range: ClassVar[str] = "port must be in range [1, 65535]"

    def __post_init__(self) -> None:
        self.__fail_if(self.api_prefix.endswith("/"), self.__prefix_tail)
        self.__fail_if((bool(self.api_prefix) and not
                        self.api_prefix.startswith("/")), self.__prefix_head)
        self.__fail_if(not (0 < self.uvi_port < 65536), self.__port_range)
        self.__fail_if(self.uvi_workers < 1, self.__workers_neg)

    @staticmethod
    def __fail_if(cond: bool, msg: str) -> None:
        if cond:
            raise ValueError(msg)

    @property
    def needs_import_string(self) -> bool:
        """uvicorn cannot reload or fork workers from an app object."""
        return self.uvi_reload or self.uvi_workers > 1

    def to_api_kwargs(self) -> dict[str, Any]:
        return {
            "title": self.api_title,
            "version": self.api_version,
            "description": self.api_description,
        }

    def to_uvi_kwargs(self) -> dict[str, Any]:
        return {
            "host": self.uvi_host,
            "port": self.uvi_port,
            "log_level": self.uvi_level,
            "reload": self.uvi_reload,
            "workers": self.uvi_workers,
            "proxy_headers": self.uvi_proxy_headers,
            "forwarded_allow_ips": self.uvi_forwarded_allow_ips,
            "timeout_keep_alive": self.uvi_timeout_keep_alive,
            "access_log": self.uvi_access_log,
        }


class WebhookFeature(ABC):
    _final: ClassVar[frozenset[str]] = frozenset({
        "_final", "__init__", "router", "finalize_routes", "install_route",
    })

    def __init_subclass__(cls, **kw: Any) -> None:
        if clash := (cls._final & cls.__dict__.keys()):
            collisions = ", ".join(sorted(clash))
            raise TypeError(f"{cls.__name__} may not override {collisions}")
        super().__init_subclass__(**kw)

    @final
    def __init__(self, router: APIRouter, *args: Any, **kwargs: Any) -> None:
        self.__installed = False
        self.__router = router
        self.install_feature(*args, **kwargs)

    @final
    @property
    def router(self) -> APIRouter:
        if self.__installed:
            raise RuntimeError("cannot access 'router' after server start")
        return self.__router

    @final
    def finalize_routes(self) -> None:
        self.__installed = True

    @final
    def install_route(self, ep: str, cb: Callable, meths: list[str]) -> None:
        if self.__installed:
            raise RuntimeError(f"cannot install {ep!r} after server start")
        self.__router.add_api_route(ep, cb, methods=meths)

    @abstractmethod
    def install_feature(self, *args: Any, **kwargs: Any) -> None:
        """This method gets automatically called during instantiation, and it
        is the only method that's guaranteed to get called before the server
        is started. Because the method is called in __init__, subclasses can
        create class variables from inside this call.
        
        Subclasses are required to override this method and should use it to
        install endpoint routes using the `install_route` method."""

    @asynccontextmanager
    async def lifespan(self, app: FastAPI) -> AsyncGenerator[None]:
        """Acquire resources before `yield`, release after. This runs after
        server start but before requests are served. these resources are not
        not acquired in a per-request context, so they are used async and
        should be expected to persist across the server's lifespan.
        
        Default behavior is to do nothing, but this method can be overridden
        by features that need it."""
        yield


class WebhookServer:
    def __init__(self, configuration: WebhookConfig) -> None:
        self._config = configuration
        self._app = FastAPI(lifespan=self._lifespan,
                            **self._config.to_api_kwargs())
        self._router = APIRouter(prefix=self._config.api_prefix)
        self._features: list[WebhookFeature] = []

    @asynccontextmanager
    async def _lifespan(self, app: FastAPI) -> AsyncGenerator[None]:
        async with AsyncExitStack() as stack:
            for feature in self._features:
                await stack.enter_async_context(feature.lifespan(app))
            yield

    def enable(self,
        feat: type[WebhookFeature],
        *args: Any,
        **kwargs: Any
    ) -> WebhookFeature:
        feature = feat(self._router, *args, **kwargs)
        self._features.append(feature)
        return feature

    def start(self) -> None:
        if self._config.needs_import_string:
            raise RuntimeError("uvicorn reload / workers not implemented")
        for feature in self._features:
            feature.finalize_routes()
        self._app.include_router(self._router)
        uvicorn.run(self._app, **self._config.to_uvi_kwargs())
