#include <stddef.h>

#ifndef GO_CGO_EXPORT_PROLOGUE_H
#define GO_CGO_EXPORT_PROLOGUE_H

#ifndef GO_CGO_GOSTRING_TYPEDEF
typedef struct { const char *p; ptrdiff_t n; } _GoString_;
extern size_t _GoStringLen(_GoString_ s);
extern const char *_GoStringPtr(_GoString_ s);
#endif

#endif

#include <string.h>
//在Go1.6.2版本之后，Go的runtime 加入了指针违规传递检测机制。该机制主要针对Go向C传递带有指向其他Go内存的地址，具体见文献检查机制。
//Go 1.25.9 支持自动将此处代码生成.h文件，因此，我们不再引用.h文件，而是将必须的内容书写在此

enum LogLevel {
    LevelFatal = 0,
    LevelError,
    LevelWarn,
    LevelInfo,
    LevelDebug,
    LevelTrace
};
//声明消息回调
typedef void (*MessageCallback)(const char*);
typedef void (*MessageChannelCallback)(const char*,int);

typedef struct _NetworkData {
    int len;
    char *ptr;
} NetworkData;

typedef struct _ClientData {
	char* id;
	int index;
    int len;
    char* ptr;
} ClientData;

enum NetworkResult {
    Success = 0,
    Error=80000,
    ErrorContext,
    ErrorParam,
    ErrorSocket,
    ErrorBuffer,
    ErrorClose,
    Closed
};


#ifndef GO_CGO_PROLOGUE_H
#define GO_CGO_PROLOGUE_H

static void callMessageCallback(MessageCallback callback,const char* msg){
	if(callback){
		callback(msg);
	}
}

static void callMessageChannelCallback(MessageChannelCallback callback,const char* msg,int channelId){
	if(callback){
		callback(msg,channelId);
	}
}

typedef signed char GoInt8;
typedef unsigned char GoUint8;
typedef short GoInt16;
typedef unsigned short GoUint16;
typedef int GoInt32;
typedef unsigned int GoUint32;
typedef long long GoInt64;
typedef unsigned long long GoUint64;
typedef GoInt64 GoInt;
typedef GoUint64 GoUint;
typedef size_t GoUintptr;
typedef float GoFloat32;
typedef double GoFloat64;
#ifdef _MSC_VER
#if !defined(__cplusplus) || _MSVC_LANG <= 201402L
#include <complex.h>
typedef _Fcomplex GoComplex64;
typedef _Dcomplex GoComplex128;
#else
#include <complex>
typedef std::complex<float> GoComplex64;
typedef std::complex<double> GoComplex128;
#endif
#else
typedef float _Complex GoComplex64;
typedef double _Complex GoComplex128;
#endif

/*
  static assertion to make sure the file is being used on architecture
  at least with matching size of GoInt.
*/
typedef char _check_for_64_bit_pointer_matching_GoInt[sizeof(void*)==64/8 ? 1:-1];

#ifndef GO_CGO_GOSTRING_TYPEDEF
typedef _GoString_ GoString;
#endif
typedef void *GoMap;
typedef void *GoChan;
typedef struct { void *t; void *v; } GoInterface;
typedef struct { void *data; GoInt len; GoInt cap; } GoSlice;

#endif

/* End of boilerplate cgo prologue.  */

#ifdef __cplusplus
extern "C" {
#endif

void InitLogCallback(int level, MessageCallback callback);
int InitNetwork(void);

int SetOnAcceptSocketCallback(MessageCallback callback);
int SetOnDisConnectedCallback(MessageCallback callback);

int ClientClose(void);
int ClientConnect(int channelCount, NetworkData* config);
int ClientChannelReceive(int chnIdx, NetworkData* data);
int ClientChannelSend(int chnIdx, int idr, NetworkData* data);
int ClientChannelClose(int chnIdx);

int ServerCreate(NetworkData* config);
int ServerClose(void);
int ServerStartListen(void);
int ServerSocketClose(char* clientId);
int ServerSocketSend(char* clientId, int chnIdx,int idr, NetworkData* data);
int ServerSocketReceive(ClientData* data);
int ServerSocketChannelReceive(char* clientId, int chnIdx, NetworkData* data);

int ProxyServerCreate(NetworkData* config);
int ProxyServerSocketClose(char* clientId);

int WebSocketCreate(void);
int WebSocketConnect(NetworkData* config);
int WebSocketClose(void);
int WebSocketSend(char* msg);
void SetOnWebSocketMessageCallback(MessageCallback callback);
void SetOnWebSocketConnectedCallback(MessageCallback callback);
void SetOnWebSocketDisconnectedCallback(MessageCallback callback);

#ifdef __cplusplus
}
#endif
