import 'dart:async';
import 'dart:io';

import 'package:flutter/services.dart';
import 'package:flutter_compass/flutter_compass.dart';
import 'package:flutter_qiblah/src/utils.dart';
import 'package:location/location.dart';
import 'package:stream_transform/stream_transform.dart' show CombineLatest;

/// [FlutterQiblah] is a singleton class that provides assess to compass events,
/// check for sensor support in Android
/// Get current  location
/// Get Qiblah direction
class FlutterQiblah {
  static const _channel = const MethodChannel('ml.medyas.flutter_qiblah');

  static final _instance = FlutterQiblah._();

  Stream<QiblahDirection>? _qiblahStream;
  static final _location = Location();

  FlutterQiblah._();

  factory FlutterQiblah() => _instance;

  /// Check Android device sensor support
  static Future<bool?> androidDeviceSensorSupport() async {
    if (Platform.isAndroid)
      return await _channel.invokeMethod("androidSupportSensor");
    else
      return true;
  }

  /// Request Location permission, return GeolocationStatus object
  static Future<PermissionStatus> requestPermissions() =>
      _location.requestPermission();

  /// get location status: GPS enabled and the permission status with GeolocationStatus
  static Future<LocationStatus> checkLocationStatus() async {
    final status = await _location.hasPermission();

    final enabled = await _location.serviceEnabled();
    return LocationStatus(enabled, status);
  }

  /// Provides a stream of Map with current compass and Qiblah direction
  /// {"qiblah": QIBLAH, "direction": DIRECTION}
  /// Direction varies from 0-360, 0 being north.
  /// Qiblah varies from 0-360, offset from direction(North)
  static Stream<QiblahDirection> get qiblahStream {
    _instance._qiblahStream ??= _merge<CompassEvent, LocationData>(
      FlutterCompass.events!,
      _location.onLocationChanged.transform(
        StreamTransformer<LocationData, LocationData>.fromHandlers(
          handleData: (LocationData position, EventSink<LocationData> sink) {
            if (position.latitude != null && position.longitude != null) {
              sink.add(position);
              sink.close();
            }
          },
        ),
      ),
    );

    return _instance._qiblahStream!;
  }

  /// Merge the compass stream with location updates, and calculate the Qiblah direction
  /// return a Stream<Map<String, dynamic>> containing compass and Qiblah direction
  /// Direction varies from 0-360, 0 being north.
  /// Qiblah varies from 0-360, offset from direction(North)
  static Stream<QiblahDirection> _merge<A, B>(
    Stream<A> streamA,
    Stream<B> streamB,
  ) =>
      streamA.combineLatest<B, QiblahDirection>(
        streamB,
        (dir, pos) {
          final position = pos as LocationData;
          final event = dir as CompassEvent;
          // Calculate the Qiblah offset to North
          final offSet = Utils.getOffsetFromNorth(
            position.latitude!,
            position.longitude!,
          );

          // Adjust Qiblah direction based on North direction
          final qiblah = (event.heading ?? 0.0) + (360 - offSet);

          return QiblahDirection(qiblah, event.heading ?? 0.0, offSet);
        },
      );

  /// Close compass stream, and set Qiblah stream to null
  void dispose() {
    _qiblahStream = null;
  }
}

/// Location Status class, contains the GPS status(Enabled or not) and GeolocationStatus
class LocationStatus {
  final bool enabled;
  final PermissionStatus status;

  const LocationStatus(
    this.enabled,
    this.status,
  );
}

/// Containing Qiblah, Direction and offset
class QiblahDirection {
  final double qiblah;
  final double direction;
  final double offset;

  const QiblahDirection(
    this.qiblah,
    this.direction,
    this.offset,
  );
}
